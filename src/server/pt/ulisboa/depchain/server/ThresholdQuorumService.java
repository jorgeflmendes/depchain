package pt.ulisboa.depchain.server;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingDeque;

import com.weavechain.curve25519.Scalar;

import pt.ulisboa.depchain.server.Message.MessageType;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.utils.SerializationUtil;
import pt.ulisboa.depchain.shared.utils.ThresholdCryptoUtil;
import pt.ulisboa.depchain.shared.utils.ThresholdCryptoUtil.ThresholdNonceShare;
import pt.ulisboa.depchain.shared.utils.ThresholdCryptoUtil.ThresholdPartialSignContext;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class ThresholdQuorumService {
  private static final int GENESIS_VIEW_NUMBER = -1;

  private final int localSenderId;
  private final int totalReplicas;
  private final int threshold;
  private final int localReplicaIndex;
  private final Scalar localThresholdShare;
  private final byte[] publicThresholdKey;
  private final ConfigParser config;

  private AuthenticatedLink nodeTransport;

  ThresholdQuorumService(int localSenderId, ConfigParser config, Scalar localThresholdShare, byte[] publicThresholdKey) {
    this.config = ValidationUtils.requireNonNull(config, "config");
    this.localSenderId = localSenderId;
    this.totalReplicas = config.system().n();
    this.threshold = config.system().n() - config.system().f();
    this.localThresholdShare = ValidationUtils.requireNonNull(localThresholdShare, "localThresholdShare");
    this.publicThresholdKey = ValidationUtils.requireNonNull(publicThresholdKey, "publicThresholdKey");
    this.localReplicaIndex = replicaIndex(localSenderId);
  }

  void initTransport(AuthenticatedLink nodeTransport) {
    this.nodeTransport = ValidationUtils.requireNonNull(nodeTransport, "nodeTransport");
  }

  Message createVoteMessage(int viewNumber, MessageType type, Node node, QuorumCertificate qc, int leaderId, BlockingDeque<Message> messageQueue) {
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");
    ValidationUtils.requireAllNonNull(ValidationUtils.named("type", type), ValidationUtils.named("node", node), ValidationUtils.named("messageQueue", messageQueue));
    ValidationUtils.requireNonNegativeInt(leaderId, "leaderId");

    Message vote = new Message(viewNumber, localSenderId, type, node, qc);

    try {
      byte[] payload = buildVotePayload(type, viewNumber, node);
      ThresholdNonceShare nonceShare = ThresholdCryptoUtil.thresholdNonceShare(payload, localThresholdShare);

      Message commitment = new Message(viewNumber, localSenderId, type, node, null);
      commitment.setPartialCommitment(nonceShare.commitment());
      sendToReplica(leaderId, commitment);

      Message contextMessage = waitForThresholdContext(type, viewNumber, leaderId, node, messageQueue);
      Set<Integer> participantIndexes = new LinkedHashSet<>();
      for (int participantIndex : contextMessage.getParticipantIndexes()) {
        participantIndexes.add(participantIndex);
      }

      ThresholdPartialSignContext context = new ThresholdPartialSignContext(localReplicaIndex, totalReplicas, threshold, participantIndexes, publicThresholdKey,
          contextMessage.getAggregatedCommitment());
      vote.setSignature(ThresholdCryptoUtil.thresholdPartialSign(payload, localThresholdShare, nonceShare, context));
      return vote;
    } catch (Exception exception) {
      throw new IllegalStateException("Unable to produce threshold vote for " + type + " at view " + viewNumber, exception);
    }
  }

  QuorumCertificate buildQC(int viewNumber, MessageType type, Node node, BlockingDeque<Message> messageQueue) {
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");
    ValidationUtils.requireAllNonNull(ValidationUtils.named("type", type), ValidationUtils.named("node", node), ValidationUtils.named("messageQueue", messageQueue));

    try {
      List<Message> commitmentMessages = waitForThresholdCommitments(type, viewNumber, node, messageQueue);
      int[] participantIndexes = new int[commitmentMessages.size()];
      List<byte[]> commitments = new ArrayList<>(commitmentMessages.size());
      Set<Integer> participantSenderIds = new LinkedHashSet<>();

      for (int i = 0; i < commitmentMessages.size(); i++) {
        Message message = commitmentMessages.get(i);
        participantIndexes[i] = replicaIndex(message.getSenderId());
        commitments.add(message.getPartialCommitment());
        participantSenderIds.add(message.getSenderId());
      }

      byte[] aggregatedCommitment = ThresholdCryptoUtil.thresholdAggregateCommitments(totalReplicas, threshold, commitments);
      for (Integer participantSenderId : participantSenderIds) {
        Message contextMessage = new Message(viewNumber, localSenderId, type, node, null);
        contextMessage.setThresholdContext(aggregatedCommitment, participantIndexes);
        sendToReplica(participantSenderId, contextMessage);
      }

      List<Message> signatureMessages = waitForThresholdPartialSignatures(type, viewNumber, node, participantSenderIds, messageQueue);
      List<byte[]> partialSignatures = new ArrayList<>(signatureMessages.size());
      for (Message message : signatureMessages) {
        partialSignatures.add(message.getSignature());
      }

      byte[] aggregatedSignature = ThresholdCryptoUtil.thresholdCombinePartialSignatures(totalReplicas, threshold, aggregatedCommitment, partialSignatures);
      if (!ThresholdCryptoUtil.verifyThresholdSignature(buildVotePayload(type, viewNumber, node), aggregatedSignature, publicThresholdKey)) {
        throw new IllegalStateException("Combined threshold signature failed verification for " + type + " at view " + viewNumber);
      }

      QuorumCertificate qc = new QuorumCertificate(type, viewNumber, node);
      qc.setAggregatedSignature(aggregatedSignature);
      return qc;
    } catch (Exception exception) {
      throw new IllegalStateException("Unable to build quorum certificate for " + type + " at view " + viewNumber, exception);
    }
  }

  boolean verifyQC(QuorumCertificate qc) {
    if (qc == null) {
      return false;
    }
    if (sameNode(qc.getNode(), Node.GENESIS_NODE) && qc.getAggregatedSignature() == null && qc.getViewNumber() == GENESIS_VIEW_NUMBER) {
      return true;
    }
    if (qc.getNode() == null || qc.getAggregatedSignature() == null) {
      return false;
    }

    try {
      return ThresholdCryptoUtil.verifyThresholdSignature(buildVotePayload(qc.getType(), qc.getViewNumber(), qc.getNode()), qc.getAggregatedSignature(), publicThresholdKey);
    } catch (Exception exception) {
      return false;
    }
  }

  boolean isAuxiliaryMessage(Message msg) {
    return msg != null && msg.getThresholdPayloadType() != Message.ThresholdPayloadType.HOTSTUFF;
  }

  QuorumCertificate genesisQC() {
    return new QuorumCertificate(MessageType.DECIDE, GENESIS_VIEW_NUMBER, Node.GENESIS_NODE);
  }

  private List<Message> waitForThresholdCommitments(MessageType type, int viewNumber, Node node, BlockingDeque<Message> messageQueue) {
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");
    ValidationUtils.requireAllNonNull(ValidationUtils.named("type", type), ValidationUtils.named("node", node), ValidationUtils.named("messageQueue", messageQueue));

    LinkedHashMap<Integer, Message> commitmentsBySender = new LinkedHashMap<>();
    while (commitmentsBySender.size() < threshold) {
      Message msg = takeMessage(messageQueue);
      if (msg == null) {
        continue;
      }

      if (msg.getType() == type && msg.getCurrView() == viewNumber && sameNode(msg.getNode(), node)
          && msg.getThresholdPayloadType() == Message.ThresholdPayloadType.SIGNATURE_COMMITMENT) {
        commitmentsBySender.putIfAbsent(msg.getSenderId(), msg);
      } else {
        messageQueue.addLast(msg);
      }
    }
    return new ArrayList<>(commitmentsBySender.values());
  }

  private List<Message> waitForThresholdPartialSignatures(MessageType type, int viewNumber, Node node, Set<Integer> expectedSenderIds, BlockingDeque<Message> messageQueue) {
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");
    ValidationUtils.requireAllNonNull(ValidationUtils.named("type", type), ValidationUtils.named("node", node), ValidationUtils
        .named("expectedSenderIds", expectedSenderIds), ValidationUtils.named("messageQueue", messageQueue));
    ValidationUtils.requireNonEmpty(expectedSenderIds, "expectedSenderIds");

    LinkedHashMap<Integer, Message> signaturesBySender = new LinkedHashMap<>();
    while (signaturesBySender.size() < expectedSenderIds.size()) {
      Message msg = takeMessage(messageQueue);
      if (msg == null) {
        continue;
      }

      if (msg.getType() == type && msg.getCurrView() == viewNumber && sameNode(msg.getNode(), node) && msg.getThresholdPayloadType() == Message.ThresholdPayloadType.SIGNATURE_SHARE
          && expectedSenderIds.contains(msg.getSenderId())) {
        signaturesBySender.putIfAbsent(msg.getSenderId(), msg);
      } else {
        messageQueue.addLast(msg);
      }
    }
    return new ArrayList<>(signaturesBySender.values());
  }

  private Message waitForThresholdContext(MessageType type, int viewNumber, int leaderId, Node node, BlockingDeque<Message> messageQueue) {
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");
    ValidationUtils.requireAllNonNull(ValidationUtils.named("type", type), ValidationUtils.named("node", node), ValidationUtils.named("messageQueue", messageQueue));
    ValidationUtils.requireNonNegativeInt(leaderId, "leaderId");

    while (true) {
      Message msg = takeMessage(messageQueue);
      if (msg == null) {
        continue;
      }

      if (msg.getSenderId() == leaderId && msg.getType() == type && msg.getCurrView() == viewNumber && sameNode(msg.getNode(), node)
          && msg.getThresholdPayloadType() == Message.ThresholdPayloadType.SIGNATURE_CONTEXT) {
        return msg;
      } else {
        messageQueue.addLast(msg);
      }
    }
  }

  private Message takeMessage(BlockingDeque<Message> messageQueue) {
    try {
      return messageQueue.take();
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  private void sendToReplica(int senderId, Message msg) throws Exception {
    ValidationUtils.requireNonNegativeInt(senderId, "senderId");
    ValidationUtils.requireNonNull(msg, "msg");
    ValidationUtils.requireNonNull(nodeTransport, "nodeTransport");

    ConfigParser.ReplicaSection replica = replica(senderId);
    byte[] payload = SerializationUtil.encodeMessage(msg);
    InetAddress host = InetAddress.getByName(replica.host());
    java.net.InetSocketAddress address = new java.net.InetSocketAddress(host, replica.consensusPort());
    nodeTransport.send(0L, payload, address);
  }

  private byte[] buildVotePayload(MessageType type, int viewNumber, Node node) {
    ValidationUtils.requireNonNull(type, "type");
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");

    String nodeHash = "null";
    if (node != null) {
      nodeHash = node.getThisHash();
    }
    return (type.name() + "|" + viewNumber + "|" + nodeHash).getBytes(StandardCharsets.UTF_8);
  }

  private boolean sameNode(Node left, Node right) {
    if (left == null || right == null) {
      return left == right;
    }
    return left.getThisHash().equals(right.getThisHash());
  }

  private int replicaIndex(int senderId) {
    ValidationUtils.requireNonNegativeInt(senderId, "senderId");

    for (int i = 0; i < config.replicas().size(); i++) {
      if (config.replicas().get(i).senderId() == senderId) {
        return i;
      }
    }

    throw new IllegalArgumentException("Unknown replica senderId: " + senderId);
  }

  private ConfigParser.ReplicaSection replica(int senderId) {
    ValidationUtils.requireNonNegativeInt(senderId, "senderId");

    for (ConfigParser.ReplicaSection replica : config.replicas()) {
      if (replica.senderId() == senderId) {
        return replica;
      }
    }

    throw new IllegalArgumentException("Unknown replica senderId: " + senderId);
  }
}
