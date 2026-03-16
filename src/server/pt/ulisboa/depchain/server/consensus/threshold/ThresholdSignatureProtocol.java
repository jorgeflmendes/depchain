package pt.ulisboa.depchain.server.consensus.threshold;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import com.weavechain.curve25519.Scalar;

import pt.ulisboa.depchain.proto.ConsensusMessageType;
import pt.ulisboa.depchain.proto.Message;
import pt.ulisboa.depchain.proto.Node;
import pt.ulisboa.depchain.proto.QuorumCertificate;
import pt.ulisboa.depchain.proto.VoteMessage;
import pt.ulisboa.depchain.server.consensus.ConsensusUtil;
import pt.ulisboa.depchain.server.consensus.ViewChangeTimeoutException;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.utils.ConsensusPayloadUtil;
import pt.ulisboa.depchain.shared.utils.TimeUtil;
import pt.ulisboa.depchain.shared.utils.ThresholdCryptoUtil;
import pt.ulisboa.depchain.shared.utils.ThresholdCryptoUtil.ThresholdNonceShare;
import pt.ulisboa.depchain.shared.utils.ThresholdCryptoUtil.ThresholdPartialSignContext;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class ThresholdSignatureProtocol {
  private static final int GENESIS_VIEW_NUMBER = -1;

  private final int localReplicaIndex;
  private final int localSenderId;
  private final int totalReplicas;
  private final int threshold;
  private final long thresholdRoundTimeoutMs;
  private final Scalar localThresholdShare;
  private final byte[] publicThresholdKey;
  private final ThresholdSignatureExchange messageExchange;

  public ThresholdSignatureProtocol(int localSenderId, ConfigParser config, Scalar localThresholdShare, byte[] publicThresholdKey) {
    ValidationUtils.requireAllNonNull(
        ValidationUtils.named("config", config),
        ValidationUtils.named("localThresholdShare", localThresholdShare),
        ValidationUtils.named("publicThresholdKey", publicThresholdKey));

    this.localSenderId = localSenderId;
    this.totalReplicas = config.system().n();
    this.threshold = config.system().n() - config.system().f();
    this.thresholdRoundTimeoutMs = config.timeouts().thresholdRoundMs();
    this.localReplicaIndex = findReplicaIndex(config, localSenderId);
    this.localThresholdShare = localThresholdShare;
    this.publicThresholdKey = publicThresholdKey;
    this.messageExchange = new ThresholdSignatureExchange(config, localSenderId, config.timeouts().thresholdRoundMs());
  }

  public void initTransport(AuthenticatedLink nodeTransport) {
    messageExchange.initTransport(nodeTransport);
  }

  public QuorumCertificate genesisQC() {
    return QuorumCertificate.newBuilder().setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_DECIDE).setViewNumber(GENESIS_VIEW_NUMBER)
        .setCertifiedNode(ConsensusUtil.GENESIS_NODE).build();
  }

  public Message createVoteMessage(int viewNumber, ConsensusMessageType type, Node node, int leaderId, BlockingDeque<Message> messageQueue) {
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");
    ValidationUtils.requireAllNonNull(ValidationUtils.named("type", type), ValidationUtils.named("node", node), ValidationUtils.named("messageQueue", messageQueue));
    ValidationUtils.requireNonNegativeInt(leaderId, "leaderId");

    try {
      byte[] payload = buildVotePayload(type, viewNumber, node);
      ThresholdNonceShare nonceShare = ThresholdCryptoUtil.thresholdNonceShare(payload, localThresholdShare);
      messageExchange.sendCommitment(leaderId, viewNumber, localSenderId, type, node, nonceShare.commitment());

      ThresholdSignatureExchange.ThresholdContext context = messageExchange.waitForContext(type, viewNumber, leaderId, node, messageQueue);
      ThresholdPartialSignContext signContext = new ThresholdPartialSignContext(localReplicaIndex, totalReplicas, threshold, context.participantIndexes(), publicThresholdKey,
          context.aggregatedCommitment());
      byte[] thresholdSignatureShare = ThresholdCryptoUtil.thresholdPartialSign(payload, localThresholdShare, nonceShare, signContext);
      return Message.newBuilder().setViewNumber(viewNumber).setReplicaSenderId(localSenderId).setMessageType(type)
          .setVote(VoteMessage.newBuilder().setVotedNode(node).setThresholdSignatureShare(ByteString.copyFrom(thresholdSignatureShare))
              .setAggregatedCommitment(ByteString.copyFrom(context.aggregatedCommitment())))
          .build();
    } catch (ViewChangeTimeoutException exception) {
      throw exception;
    } catch (Exception exception) {
      throw new IllegalStateException("Unable to produce threshold vote for " + type + " at view " + viewNumber, exception);
    }
  }

  public QuorumCertificate buildQC(int viewNumber, ConsensusMessageType type, Node node, BlockingDeque<Message> messageQueue) {
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");
    ValidationUtils.requireAllNonNull(ValidationUtils.named("type", type), ValidationUtils.named("node", node), ValidationUtils.named("messageQueue", messageQueue));

    try {
      long overallDeadlineNanos = TimeUtil.monotonicDeadlineAfterNow(thresholdRoundTimeoutMs);
      long commitmentDeadlineNanos = Math.min(overallDeadlineNanos, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(1L, thresholdRoundTimeoutMs / 2)));
      byte[] payload = buildVotePayload(type, viewNumber, node);
      ThresholdNonceShare localNonceShare = ThresholdCryptoUtil.thresholdNonceShare(payload, localThresholdShare);
      List<ThresholdSignatureExchange.RemoteCommitment> remoteCommitments =
          messageExchange.collectCommitments(type, viewNumber, node, threshold - 1, messageQueue, commitmentDeadlineNanos);

      List<CommitmentBatch> candidateBatches = buildCandidateCommitmentBatches(localNonceShare.commitment(), remoteCommitments);
      for (int batchIndex = 0; batchIndex < candidateBatches.size(); batchIndex++) {
        CommitmentBatch batch = candidateBatches.get(batchIndex);
        long attemptDeadlineNanos = subAttemptDeadline(overallDeadlineNanos, candidateBatches.size() - batchIndex);

        try {
          byte[] aggregatedCommitment = ThresholdCryptoUtil.thresholdAggregateCommitments(totalReplicas, threshold, batch.commitments());
          messageExchange.sendContextMessages(batch.participantSenders(), viewNumber, localSenderId, type, node, aggregatedCommitment, batch.participantIndexes());

          ThresholdPartialSignContext localContext = new ThresholdPartialSignContext(localReplicaIndex, totalReplicas, threshold, batch.participantIndexesSet(), publicThresholdKey,
              aggregatedCommitment);
          List<byte[]> partialSignatures = new ArrayList<>(threshold);
          partialSignatures.add(ThresholdCryptoUtil.thresholdPartialSign(payload, localThresholdShare, localNonceShare, localContext));
          partialSignatures.addAll(messageExchange.collectSignatureShares(type, viewNumber, node, aggregatedCommitment, batch.participantSenders(), messageQueue,
              attemptDeadlineNanos));

          byte[] aggregatedSignature = ThresholdCryptoUtil.thresholdCombinePartialSignatures(totalReplicas, threshold, aggregatedCommitment, partialSignatures);
          if (ThresholdCryptoUtil.verifyThresholdSignature(payload, aggregatedSignature, publicThresholdKey)) {
            return QuorumCertificate.newBuilder().setMessageType(type).setViewNumber(viewNumber).setCertifiedNode(node).setQuorumSignature(ByteString.copyFrom(aggregatedSignature))
                .build();
          }
        } catch (ViewChangeTimeoutException | IllegalStateException ignored) {
          // Try the next participant subset within the same overall deadline.
        }
      }

      throw new ViewChangeTimeoutException("Unable to assemble a valid threshold quorum certificate before the view deadline");
    } catch (ViewChangeTimeoutException exception) {
      throw exception;
    } catch (Exception exception) {
      throw new IllegalStateException("Unable to build quorum certificate for " + type + " at view " + viewNumber, exception);
    }
  }

  public boolean verifyQC(QuorumCertificate qc) {
    if (qc == null) {
      return false;
    }
    if (isSameNode(qc.getCertifiedNode(), ConsensusUtil.GENESIS_NODE) && !qc.hasQuorumSignature() && qc.getViewNumber() == GENESIS_VIEW_NUMBER) {
      return true;
    }
    if (!qc.hasCertifiedNode() || !qc.hasQuorumSignature()) {
      return false;
    }

    try {
      byte[] payload = buildVotePayload(qc.getMessageType(), qc.getViewNumber(), qc.getCertifiedNode());
      return ThresholdCryptoUtil.verifyThresholdSignature(payload, qc.getQuorumSignature().toByteArray(), publicThresholdKey);
    } catch (Exception exception) {
      return false;
    }
  }

  public boolean isAuxiliaryMessage(Message msg) {
    return messageExchange.isAuxiliaryMessage(msg);
  }

  private byte[] buildVotePayload(ConsensusMessageType type, int viewNumber, Node node) {
    return ConsensusPayloadUtil.votePayload(type, viewNumber, node);
  }

  private int findReplicaIndex(ConfigParser config, int senderId) {
    return config.requireReplicaIndexForSenderId(senderId);
  }

  private List<CommitmentBatch> buildCandidateCommitmentBatches(byte[] localCommitment, List<ThresholdSignatureExchange.RemoteCommitment> remoteCommitments) {
    List<CommitmentBatch> candidateBatches = new ArrayList<>();
    buildCommitmentBatchCombinations(localCommitment, remoteCommitments, 0, new ArrayList<>(), candidateBatches);
    return candidateBatches;
  }

  private void buildCommitmentBatchCombinations(byte[] localCommitment, List<ThresholdSignatureExchange.RemoteCommitment> remoteCommitments, int nextIndex,
      List<ThresholdSignatureExchange.RemoteCommitment> selectedRemoteCommitments, List<CommitmentBatch> candidateBatches) {
    if (selectedRemoteCommitments.size() == threshold - 1) {
      candidateBatches.add(newCommitmentBatch(localCommitment, selectedRemoteCommitments));
      return;
    }

    int remainingRequired = (threshold - 1) - selectedRemoteCommitments.size();
    for (int i = nextIndex; i <= remoteCommitments.size() - remainingRequired; i++) {
      selectedRemoteCommitments.add(remoteCommitments.get(i));
      buildCommitmentBatchCombinations(localCommitment, remoteCommitments, i + 1, selectedRemoteCommitments, candidateBatches);
      selectedRemoteCommitments.remove(selectedRemoteCommitments.size() - 1);
    }
  }

  private CommitmentBatch newCommitmentBatch(byte[] localCommitment, List<ThresholdSignatureExchange.RemoteCommitment> selectedRemoteCommitments) {
    int[] participantIndexes = new int[threshold];
    List<byte[]> commitments = new ArrayList<>(threshold);
    Set<Integer> participantIndexesSet = new LinkedHashSet<>();
    Set<Integer> participantSenders = new LinkedHashSet<>();

    participantIndexes[0] = localReplicaIndex;
    commitments.add(localCommitment);
    participantIndexesSet.add(localReplicaIndex);

    for (int i = 0; i < selectedRemoteCommitments.size(); i++) {
      ThresholdSignatureExchange.RemoteCommitment remoteCommitment = selectedRemoteCommitments.get(i);
      participantIndexes[i + 1] = remoteCommitment.participantIndex();
      commitments.add(remoteCommitment.commitment());
      participantIndexesSet.add(remoteCommitment.participantIndex());
      participantSenders.add(remoteCommitment.senderId());
    }

    return new CommitmentBatch(commitments, participantIndexes, participantIndexesSet, participantSenders);
  }

  private long subAttemptDeadline(long overallDeadlineNanos, int remainingAttempts) {
    if (remainingAttempts <= 1) {
      return overallDeadlineNanos;
    }

    long remainingMs = Math.max(1L, TimeUtil.monotonicRemainingMsUntil(overallDeadlineNanos));
    long sliceMs = Math.max(1L, remainingMs / remainingAttempts);
    return Math.min(overallDeadlineNanos, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(sliceMs));
  }

  private record CommitmentBatch(List<byte[]> commitments, int[] participantIndexes, Set<Integer> participantIndexesSet, Set<Integer> participantSenders) {
  }

  static boolean isSameNode(Node left, Node right) {
    return ConsensusUtil.isSameNode(left, right);
  }
}
