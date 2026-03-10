package pt.ulisboa.depchain.server.consensus.threshold;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;

import com.weavechain.curve25519.Scalar;

import pt.ulisboa.depchain.server.consensus.Message;
import pt.ulisboa.depchain.server.consensus.Message.MessageType;
import pt.ulisboa.depchain.server.consensus.Node;
import pt.ulisboa.depchain.server.consensus.QuorumCertificate;
import pt.ulisboa.depchain.server.consensus.ViewChangeTimeoutException;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.utils.SerializationUtil;
import pt.ulisboa.depchain.shared.utils.ThresholdCryptoUtil;
import pt.ulisboa.depchain.shared.utils.ThresholdCryptoUtil.ThresholdNonceShare;
import pt.ulisboa.depchain.shared.utils.ThresholdCryptoUtil.ThresholdPartialSignContext;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class ThresholdSignatureProtocol {
  private static final int GENESIS_VIEW_NUMBER = -1;

  private final int localReplicaIndex; // Index is the position in the config file
  private final int localSenderId;

  // System parameters
  private final int totalReplicas;
  private final int threshold;

  // Keys
  private final Scalar localThresholdShare;
  private final byte[] publicThresholdKey;

  // Component responsible for the threshold message exchange
  private final ThresholdSignatureExchange messageExchange;

  public ThresholdSignatureProtocol(int localSenderId, ConfigParser config, Scalar localThresholdShare, byte[] publicThresholdKey) {
    ValidationUtils.requireAllNonNull(ValidationUtils.named("config", config), ValidationUtils.named("localThresholdShare", localThresholdShare), ValidationUtils
        .named("publicThresholdKey", publicThresholdKey));

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
    return new QuorumCertificate(MessageType.DECIDE, GENESIS_VIEW_NUMBER, Node.GENESIS_NODE);
  }

  public Message createVoteMessage(int viewNumber, MessageType type, Node node, int leaderId, BlockingDeque<Message> messageQueue) {
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");
    ValidationUtils.requireAllNonNull(ValidationUtils.named("type", type), ValidationUtils.named("node", node), ValidationUtils.named("messageQueue", messageQueue));
    ValidationUtils.requireNonNegativeInt(leaderId, "leaderId");

    try {
      Message vote = new Message(viewNumber, localSenderId, type, node, null);

      // Produce the local commitment and send it to the leader
      byte[] payload = buildVotePayload(type, viewNumber, node);
      ThresholdNonceShare nonceShare = ThresholdCryptoUtil.thresholdNonceShare(payload, localThresholdShare);
      messageExchange.sendCommitment(leaderId, viewNumber, localSenderId, type, node, nonceShare.commitment());

      // Wait for the leader to send the context and produce the local partial sign
      ThresholdSignatureExchange.ThresholdContext context = messageExchange.waitForContext(type, viewNumber, leaderId, node, messageQueue);
      ThresholdPartialSignContext signContext = new ThresholdPartialSignContext(localReplicaIndex, totalReplicas, threshold, context.participantIndexes(), publicThresholdKey,
          context.aggregatedCommitment());
      vote.setSignature(ThresholdCryptoUtil.thresholdPartialSign(payload, localThresholdShare, nonceShare, signContext));
      return vote;
    } catch (ViewChangeTimeoutException exception) {
      throw exception;
    } catch (Exception exception) {
      throw new IllegalStateException("Unable to produce threshold vote for " + type + " at view " + viewNumber, exception);
    }
  }

  public QuorumCertificate buildQC(int viewNumber, MessageType type, Node node, BlockingDeque<Message> messageQueue) {
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");
    ValidationUtils.requireAllNonNull(ValidationUtils.named("type", type), ValidationUtils.named("node", node), ValidationUtils.named("messageQueue", messageQueue));

    try {
      // Produce the local commitment and collect the commitments from the participants
      byte[] payload = buildVotePayload(type, viewNumber, node);
      ThresholdNonceShare localNonceShare = ThresholdCryptoUtil.thresholdNonceShare(payload, localThresholdShare);
      ThresholdSignatureExchange.CommitmentBatch batch = messageExchange
          .collectCommitments(localReplicaIndex, localNonceShare.commitment(), type, viewNumber, node, threshold - 1, messageQueue);

      // Aggregate the commitments and send the context to the participants
      byte[] aggregatedCommitment = ThresholdCryptoUtil.thresholdAggregateCommitments(totalReplicas, threshold, batch.commitments());
      messageExchange.sendContextMessages(batch.participantSenders(), viewNumber, localSenderId, type, node, aggregatedCommitment, batch.participantIndexes());

      // Produce the local partial sign and collect the other shares from the participants
      ThresholdPartialSignContext localContext = new ThresholdPartialSignContext(localReplicaIndex, totalReplicas, threshold, batch.participantIndexesSet(), publicThresholdKey,
          aggregatedCommitment);
      List<byte[]> partialSignatures = new ArrayList<>(threshold);
      partialSignatures.add(ThresholdCryptoUtil.thresholdPartialSign(payload, localThresholdShare, localNonceShare, localContext));
      partialSignatures.addAll(messageExchange.collectSignatureShares(type, viewNumber, node, batch.participantSenders(), messageQueue));

      // Combine the partial signs and verify the aggregated signature
      byte[] aggregatedSignature = ThresholdCryptoUtil.thresholdCombinePartialSignatures(totalReplicas, threshold, aggregatedCommitment, partialSignatures);
      if (!ThresholdCryptoUtil.verifyThresholdSignature(payload, aggregatedSignature, publicThresholdKey)) {
        throw new IllegalStateException("Combined threshold signature failed verification for " + type + " at view " + viewNumber);
      }

      // Produce the QC and return it
      QuorumCertificate qc = new QuorumCertificate(type, viewNumber, node);
      qc.setAggregatedSignature(aggregatedSignature);
      return qc;
    } catch (ViewChangeTimeoutException exception) {
      throw exception;
    } catch (Exception exception) {
      throw new IllegalStateException("Unable to build quorum certificate for " + type + " at view " + viewNumber, exception);
    }
  }

  public boolean verifyQC(QuorumCertificate qc) {
    if (qc == null) {
      return false;
    } else if (isSameNode(qc.getNode(), Node.GENESIS_NODE) && qc.getAggregatedSignature() == null && qc.getViewNumber() == GENESIS_VIEW_NUMBER) {
      return true;
    } else if (qc.getNode() == null || qc.getAggregatedSignature() == null) {
      return false;
    }

    // Neither genesis nor null, so we need to verify the signature
    try {
      byte[] payload = buildVotePayload(qc.getType(), qc.getViewNumber(), qc.getNode());
      return ThresholdCryptoUtil.verifyThresholdSignature(payload, qc.getAggregatedSignature(), publicThresholdKey);
    } catch (Exception exception) {
      return false;
    }
  }

  public boolean isAuxiliaryMessage(Message msg) {
    return messageExchange.isAuxiliaryMessage(msg);
  }

  private byte[] buildVotePayload(MessageType type, int viewNumber, Node node) {
    return SerializationUtil.encodeVotePayload(type, viewNumber, node);
  }

  private int findReplicaIndex(ConfigParser config, int senderId) {
    for (int i = 0; i < config.replicas().size(); i++) {
      if (config.replicas().get(i).senderId() == senderId) {
        return i;
      }
    }

    throw new IllegalArgumentException("Unknown replica senderId: " + senderId);
  }

  static boolean isSameNode(Node left, Node right) {
    if (left == null || right == null) {
      return left == right;
    }

    return left.getThisHash().equals(right.getThisHash());
  }
}
