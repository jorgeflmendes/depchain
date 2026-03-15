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

import com.google.protobuf.ByteString;

import pt.ulisboa.depchain.proto.CommitmentMessage;
import pt.ulisboa.depchain.proto.ConsensusMessageType;
import pt.ulisboa.depchain.proto.Message;
import pt.ulisboa.depchain.proto.Node;
import pt.ulisboa.depchain.proto.ThresholdContextMessage;
import pt.ulisboa.depchain.proto.ThresholdSignatureContext;
import pt.ulisboa.depchain.server.consensus.ConsensusUtil;
import pt.ulisboa.depchain.server.consensus.ViewChangeTimeoutException;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.utils.ProtoValidationUtil;
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
    return msg != null && ConsensusUtil.isAuxiliaryMessage(msg);
  }

  void sendCommitment(int leaderId, int viewNumber, int senderId, ConsensusMessageType type, Node node, byte[] commitment) throws Exception {
    ValidationUtils.requireNonNegativeInt(leaderId, "leaderId");
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");
    ValidationUtils.requireNonNegativeInt(senderId, "senderId");
    ValidationUtils.requireAllNonNull(named("type", type), named("node", node), named("commitment", commitment));

    Message message = Message.newBuilder().setViewNumber(viewNumber).setReplicaSenderId(senderId).setMessageType(type)
        .setCommitment(CommitmentMessage.newBuilder().setVotedNode(node).setThresholdSignatureCommitment(ByteString.copyFrom(commitment))).build();
    sendMessage(leaderId, message);
  }

  ThresholdContext waitForContext(ConsensusMessageType type, int viewNumber, int leaderId, Node node, BlockingDeque<Message> messageQueue) {
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");
    ValidationUtils.requireNonNegativeInt(leaderId, "leaderId");
    ValidationUtils.requireAllNonNull(named("type", type), named("node", node), named("messageQueue", messageQueue));

    Message contextMessage = waitForMatchingMessage(messageQueue,
        msg -> msg.getReplicaSenderId() == leaderId && isMatchingThresholdMessage(msg, type, viewNumber, node, Message.BodyCase.THRESHOLD_CONTEXT));
    ThresholdSignatureContext context = contextMessage.getThresholdContext().getThresholdSignatureContext();
    Set<Integer> participantIndexes = new LinkedHashSet<>(context.getParticipantReplicaIndexesList());
    return new ThresholdContext(context.getAggregatedCommitment().toByteArray(), participantIndexes);
  }

  CommitmentBatch collectCommitments(int localReplicaIndex, byte[] localCommitment, ConsensusMessageType type, int viewNumber, Node node, int expectedRemoteCount,
      BlockingDeque<Message> messageQueue) {
    ValidationUtils.requireNonNegativeInt(localReplicaIndex, "localReplicaIndex");
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");
    ValidationUtils.requireNonNegativeInt(expectedRemoteCount, "expectedRemoteCount");
    ValidationUtils.requireAllNonNull(named("localCommitment", localCommitment), named("type", type), named("node", node), named("messageQueue", messageQueue));

    List<Message> commitmentMessages = collectMatchingMessages(expectedRemoteCount, messageQueue,
        msg -> isMatchingThresholdMessage(msg, type, viewNumber, node, Message.BodyCase.COMMITMENT));

    int[] participantIndexes = new int[commitmentMessages.size() + 1];
    List<byte[]> commitments = new ArrayList<>(commitmentMessages.size() + 1);
    Set<Integer> participantIndexesSet = new LinkedHashSet<>();
    Set<Integer> participantSenders = new LinkedHashSet<>();

    participantIndexes[0] = localReplicaIndex;
    commitments.add(localCommitment);
    participantIndexesSet.add(localReplicaIndex);

    for (int i = 0; i < commitmentMessages.size(); i++) {
      Message commitmentMessage = commitmentMessages.get(i);
      int participantIndex = replicaIndexForSender(commitmentMessage.getReplicaSenderId());
      participantIndexes[i + 1] = participantIndex;
      commitments.add(commitmentMessage.getCommitment().getThresholdSignatureCommitment().toByteArray());
      participantIndexesSet.add(participantIndex);
      participantSenders.add(commitmentMessage.getReplicaSenderId());
    }

    return new CommitmentBatch(commitments, participantIndexes, participantIndexesSet, participantSenders);
  }

  void sendContextMessages(Set<Integer> participantSenders, int viewNumber, int senderId, ConsensusMessageType type, Node node, byte[] aggregatedCommitment,
      int[] participantIndexes) throws Exception {
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");
    ValidationUtils.requireNonNegativeInt(senderId, "senderId");
    ValidationUtils.requireAllNonNull(
        named("participantSenders", participantSenders),
        named("type", type),
        named("node", node),
        named("aggregatedCommitment", aggregatedCommitment),
        named("participantIndexes", participantIndexes));

    for (Integer participantSenderId : participantSenders) {
      ThresholdSignatureContext context = ThresholdSignatureContext.newBuilder().setAggregatedCommitment(ByteString.copyFrom(aggregatedCommitment))
          .addAllParticipantReplicaIndexes(java.util.Arrays.stream(participantIndexes).boxed().toList()).build();
      Message message = Message.newBuilder().setViewNumber(viewNumber).setReplicaSenderId(senderId).setMessageType(type)
          .setThresholdContext(ThresholdContextMessage.newBuilder().setVotedNode(node).setThresholdSignatureContext(context)).build();
      sendMessage(participantSenderId, message);
    }
  }

  List<byte[]> collectSignatureShares(ConsensusMessageType type, int viewNumber, Node node, Set<Integer> expectedSenders, BlockingDeque<Message> messageQueue) {
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");
    ValidationUtils.requireAllNonNull(named("type", type), named("node", node), named("expectedSenders", expectedSenders), named("messageQueue", messageQueue));

    if (expectedSenders.isEmpty()) {
      return List.of();
    }

    List<Message> signatureMessages = collectMatchingMessages(expectedSenders.size(), messageQueue,
        msg -> isMatchingThresholdMessage(msg, type, viewNumber, node, Message.BodyCase.VOTE) && expectedSenders.contains(msg.getReplicaSenderId()));

    List<byte[]> signatures = new ArrayList<>(signatureMessages.size());
    for (Message signatureMessage : signatureMessages) {
      signatures.add(signatureMessage.getVote().getThresholdSignatureShare().toByteArray());
    }

    return signatures;
  }

  private void sendMessage(int senderId, Message msg) throws Exception {
    ValidationUtils.requireNonNegativeInt(senderId, "senderId");
    ValidationUtils.requireAllNonNull(named("msg", msg), named("nodeTransport", nodeTransport));

    ConfigParser.ReplicaSection replica = config.requireReplicaBySenderId(senderId);
    byte[] payload = ProtoValidationUtil.requireValid(msg, "ReplicaMessage").toByteArray();
    InetAddress host = InetAddress.getByName(replica.host());
    InetSocketAddress address = new InetSocketAddress(host, replica.consensusPort());
    nodeTransport.send(0L, payload, address);
  }

  private List<Message> collectMatchingMessages(int expectedCount, BlockingDeque<Message> messageQueue, Predicate<Message> matcher) {
    ValidationUtils.requireNonNegativeInt(expectedCount, "expectedCount");
    ValidationUtils.requireAllNonNull(named("messageQueue", messageQueue), named("matcher", matcher));

    long deadlineMs = TimeUtil.deadlineAfterNow(viewChangeTimeoutMs);
    LinkedHashMap<Integer, Message> messagesBySender = new LinkedHashMap<>();
    while (messagesBySender.size() < expectedCount) {
      Message msg = takeMessageUntilDeadline(messageQueue, deadlineMs);
      if (matcher.test(msg)) {
        messagesBySender.putIfAbsent(msg.getReplicaSenderId(), msg);
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

  private boolean isMatchingThresholdMessage(Message msg, ConsensusMessageType type, int viewNumber, Node node, Message.BodyCase bodyCase) {
    Node messageNode = switch (msg.getBodyCase()) {
      case VOTE -> msg.getVote().getVotedNode();
      case COMMITMENT -> msg.getCommitment().getVotedNode();
      case THRESHOLD_CONTEXT -> msg.getThresholdContext().getVotedNode();
      default -> null;
    };
    return msg.getMessageType() == type && msg.getViewNumber() == viewNumber && ThresholdSignatureProtocol.isSameNode(messageNode, node) && msg.getBodyCase() == bodyCase;
  }

  private int replicaIndexForSender(int senderId) {
    return config.requireReplicaIndexForSenderId(senderId);
  }
}
