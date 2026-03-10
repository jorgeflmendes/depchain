package pt.ulisboa.depchain.server.consensus.threshold;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import pt.ulisboa.depchain.server.consensus.Message;
import pt.ulisboa.depchain.server.consensus.Message.MessageType;
import pt.ulisboa.depchain.server.consensus.Node;
import pt.ulisboa.depchain.server.consensus.ViewChangeTimeoutException;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.utils.SerializationUtil;
import pt.ulisboa.depchain.shared.utils.TimeUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class ThresholdSignatureExchange {
  record ThresholdContext(byte[] aggregatedCommitment, Set<Integer> participantIndexes) {
  }

  record CommitmentBatch(List<byte[]> commitments, int[] participantIndexes, Set<Integer> participantIndexesSet, Set<Integer> participantSenders) {
  }

  private final ConfigParser config;
  private final long viewChangeTimeoutMs;

  private AuthenticatedLink nodeTransport;

  ThresholdSignatureExchange(ConfigParser config, long viewChangeTimeoutMs) {
    ValidationUtils.requireAllNonNull(named("config", config));
    ValidationUtils.requireNonNegativeLong(viewChangeTimeoutMs, "viewChangeTimeoutMs");

    this.config = config;
    this.viewChangeTimeoutMs = viewChangeTimeoutMs;
  }

  void initTransport(AuthenticatedLink nodeTransport) {
    this.nodeTransport = ValidationUtils.requireNonNull(nodeTransport, "nodeTransport");
  }

  boolean isAuxiliaryMessage(Message msg) {
    return msg != null && msg.getThresholdPayloadType() != Message.ThresholdPayloadType.HOTSTUFF;
  }

  void sendCommitment(int leaderId, int viewNumber, int senderId, MessageType type, Node node, byte[] commitment) throws Exception {
    ValidationUtils.requireNonNegativeInt(leaderId, "leaderId");
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");
    ValidationUtils.requireNonNegativeInt(senderId, "senderId");
    ValidationUtils.requireAllNonNull(named("type", type), named("node", node), named("commitment", commitment));

    Message message = new Message(viewNumber, senderId, type, node, null);
    message.setPartialCommitment(commitment);
    sendMessage(leaderId, message);
  }

  ThresholdContext waitForContext(MessageType type, int viewNumber, int leaderId, Node node, BlockingDeque<Message> messageQueue) {
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");
    ValidationUtils.requireNonNegativeInt(leaderId, "leaderId");
    ValidationUtils.requireAllNonNull(named("type", type), named("node", node), named("messageQueue", messageQueue));

    Message contextMessage = waitForMatchingMessage(messageQueue, msg -> msg.getSenderId() == leaderId
        && isMatchingThresholdMessage(msg, type, viewNumber, node, Message.ThresholdPayloadType.SIGNATURE_CONTEXT));
    Set<Integer> participantIndexes = new LinkedHashSet<>();
    for (int participantIndex : contextMessage.getParticipantIndexes()) {
      participantIndexes.add(participantIndex);
    }
    return new ThresholdContext(contextMessage.getAggregatedCommitment(), participantIndexes);
  }

  CommitmentBatch collectCommitments(int localReplicaIndex, byte[] localCommitment, MessageType type, int viewNumber, Node node, int expectedRemoteCount, BlockingDeque<Message> messageQueue) {
    ValidationUtils.requireNonNegativeInt(localReplicaIndex, "localReplicaIndex");
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");
    ValidationUtils.requireNonNegativeInt(expectedRemoteCount, "expectedRemoteCount");
    ValidationUtils.requireAllNonNull(named("localCommitment", localCommitment), named("type", type), named("node", node), named("messageQueue", messageQueue));

    List<Message> commitmentMessages = collectMatchingMessages(expectedRemoteCount, messageQueue, msg -> isMatchingThresholdMessage(msg, type, viewNumber, node, Message.ThresholdPayloadType.SIGNATURE_COMMITMENT));

    int[] participantIndexes = new int[commitmentMessages.size() + 1];
    List<byte[]> commitments = new ArrayList<>(commitmentMessages.size() + 1);
    Set<Integer> participantIndexesSet = new LinkedHashSet<>();
    Set<Integer> participantSenders = new LinkedHashSet<>();

    participantIndexes[0] = localReplicaIndex;
    commitments.add(localCommitment);
    participantIndexesSet.add(localReplicaIndex);

    for (int i = 0; i < commitmentMessages.size(); i++) {
      Message commitmentMessage = commitmentMessages.get(i);
      int participantIndex = replicaIndexForSender(commitmentMessage.getSenderId());
      participantIndexes[i + 1] = participantIndex;
      commitments.add(commitmentMessage.getPartialCommitment());
      participantIndexesSet.add(participantIndex);
      participantSenders.add(commitmentMessage.getSenderId());
    }

    return new CommitmentBatch(commitments, participantIndexes, participantIndexesSet, participantSenders);
  }

  void sendContextMessages(Set<Integer> participantSenders, int viewNumber, int senderId, MessageType type, Node node, byte[] aggregatedCommitment, int[] participantIndexes)
      throws Exception {
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");
    ValidationUtils.requireNonNegativeInt(senderId, "senderId");
    ValidationUtils
        .requireAllNonNull(named("participantSenders", participantSenders), named("type", type), named("node", node), named("aggregatedCommitment", aggregatedCommitment), named("participantIndexes", participantIndexes));

    for (Integer participantSenderId : participantSenders) {
      Message contextMessage = new Message(viewNumber, senderId, type, node, null);
      contextMessage.setThresholdContext(aggregatedCommitment, participantIndexes);
      sendMessage(participantSenderId, contextMessage);
    }
  }

  List<byte[]> collectSignatureShares(MessageType type, int viewNumber, Node node, Set<Integer> expectedSenders, BlockingDeque<Message> messageQueue) {
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");
    ValidationUtils.requireAllNonNull(named("type", type), named("node", node), named("expectedSenders", expectedSenders), named("messageQueue", messageQueue));

    if (expectedSenders.isEmpty()) {
      return List.of();
    }

    // Collects signature share messages from the expected senders.
    List<Message> signatureMessages = collectMatchingMessages(expectedSenders
        .size(), messageQueue, msg -> isMatchingThresholdMessage(msg, type, viewNumber, node, Message.ThresholdPayloadType.SIGNATURE_SHARE)
            && expectedSenders.contains(msg.getSenderId()));

    // Extracts the signature shares from the messages.
    List<byte[]> signatures = new ArrayList<>(signatureMessages.size());
    for (Message signatureMessage : signatureMessages) {
      signatures.add(signatureMessage.getSignature());
    }

    return signatures;
  }

  private void sendMessage(int senderId, Message msg) throws Exception {
    ValidationUtils.requireNonNegativeInt(senderId, "senderId");
    ValidationUtils.requireAllNonNull(named("msg", msg), named("nodeTransport", nodeTransport));

    ConfigParser.ReplicaSection replica = config.requireReplicaBySenderId(senderId);

    byte[] payload = SerializationUtil.encodeReplicaMessage(msg);
    InetAddress host = InetAddress.getByName(replica.host());
    InetSocketAddress address = new InetSocketAddress(host, replica.consensusPort());
    nodeTransport.send(0L, payload, address); // Inter replica messages can use 0.
  }

  private List<Message> collectMatchingMessages(int expectedCount, BlockingDeque<Message> messageQueue, Predicate<Message> matcher) {
    ValidationUtils.requireNonNegativeInt(expectedCount, "expectedCount");
    ValidationUtils.requireAllNonNull(named("messageQueue", messageQueue), named("matcher", matcher));

    long deadlineMs = TimeUtil.deadlineAfterNow(viewChangeTimeoutMs);
    LinkedHashMap<Integer, Message> messagesBySender = new LinkedHashMap<>();
    while (messagesBySender.size() < expectedCount) {
      Message msg = takeMessageUntilDeadline(messageQueue, deadlineMs);
      if (matcher.test(msg)) {
        messagesBySender.putIfAbsent(msg.getSenderId(), msg);
      } else {
        messageQueue.addLast(msg);
      }
    }

    return new ArrayList<>(messagesBySender.values());
  }

  private Message waitForMatchingMessage(BlockingDeque<Message> messageQueue, Predicate<Message> matcher) {
    ValidationUtils.requireAllNonNull(named("messageQueue", messageQueue), named("matcher", matcher));

    long deadlineMs = TimeUtil.deadlineAfterNow(viewChangeTimeoutMs);
    while (true) {
      Message msg = takeMessageUntilDeadline(messageQueue, deadlineMs);
      // Checks if the message matches the given predicate (condition).
      if (matcher.test(msg)) {
        return msg;
      }

      messageQueue.addLast(msg);
    }
  }

  private Message takeMessageUntilDeadline(BlockingDeque<Message> messageQueue, long deadlineMs) {
    if (TimeUtil.hasTimedOut(deadlineMs)) {
      throw new ViewChangeTimeoutException("Timed out waiting for threshold messages");
    }

    try {
      long remainingMs = TimeUtil.remainingMsUntil(deadlineMs);
      Message msg = messageQueue.poll(remainingMs, TimeUnit.MILLISECONDS);
      if (msg == null) {
        throw new ViewChangeTimeoutException("Timed out waiting for threshold messages");
      }
      return msg;
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new ViewChangeTimeoutException("Interrupted while waiting for threshold messages");
    }
  }

  private boolean isMatchingThresholdMessage(Message msg, MessageType type, int viewNumber, Node node, Message.ThresholdPayloadType payloadType) {
    return msg.getType() == type && msg.getCurrView() == viewNumber && ThresholdSignatureProtocol.isSameNode(msg.getNode(), node) && msg.getThresholdPayloadType() == payloadType;
  }

  private int replicaIndexForSender(int senderId) {
    ValidationUtils.requireNonNegativeInt(senderId, "senderId");

    for (int i = 0; i < config.replicas().size(); i++) {
      if (config.replicas().get(i).senderId() == senderId) {
        return i;
      }
    }

    throw new IllegalArgumentException("Unknown replica senderId: " + senderId);
  }

}
