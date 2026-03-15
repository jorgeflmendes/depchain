package pt.ulisboa.depchain.server.consensus.threshold;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;

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
    this.localReplicaIndex = findReplicaIndex(config, localSenderId);
    this.localThresholdShare = localThresholdShare;
    this.publicThresholdKey = publicThresholdKey;
    this.messageExchange = new ThresholdSignatureExchange(config, config.timeouts().viewChangeMs());
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
          .setVote(VoteMessage.newBuilder().setVotedNode(node).setThresholdSignatureShare(ByteString.copyFrom(thresholdSignatureShare))).build();
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
      byte[] payload = buildVotePayload(type, viewNumber, node);
      ThresholdNonceShare localNonceShare = ThresholdCryptoUtil.thresholdNonceShare(payload, localThresholdShare);
      ThresholdSignatureExchange.CommitmentBatch batch = messageExchange.collectCommitments(localReplicaIndex, localNonceShare.commitment(), type, viewNumber, node, threshold - 1,
          messageQueue);

      byte[] aggregatedCommitment = ThresholdCryptoUtil.thresholdAggregateCommitments(totalReplicas, threshold, batch.commitments());
      messageExchange.sendContextMessages(batch.participantSenders(), viewNumber, localSenderId, type, node, aggregatedCommitment, batch.participantIndexes());

      ThresholdPartialSignContext localContext = new ThresholdPartialSignContext(localReplicaIndex, totalReplicas, threshold, batch.participantIndexesSet(), publicThresholdKey,
          aggregatedCommitment);
      List<byte[]> partialSignatures = new ArrayList<>(threshold);
      partialSignatures.add(ThresholdCryptoUtil.thresholdPartialSign(payload, localThresholdShare, localNonceShare, localContext));
      partialSignatures.addAll(messageExchange.collectSignatureShares(type, viewNumber, node, batch.participantSenders(), messageQueue));

      byte[] aggregatedSignature = ThresholdCryptoUtil.thresholdCombinePartialSignatures(totalReplicas, threshold, aggregatedCommitment, partialSignatures);
      if (!ThresholdCryptoUtil.verifyThresholdSignature(payload, aggregatedSignature, publicThresholdKey)) {
        throw new IllegalStateException("Combined threshold signature failed verification for " + type + " at view " + viewNumber);
      }

      return QuorumCertificate.newBuilder().setMessageType(type).setViewNumber(viewNumber).setCertifiedNode(node).setQuorumSignature(ByteString.copyFrom(aggregatedSignature))
          .build();
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

  static boolean isSameNode(Node left, Node right) {
    return ConsensusUtil.isSameNode(left, right);
  }
}
