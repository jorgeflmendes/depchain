package pt.ulisboa.depchain.server.consensus.threshold;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import pt.ulisboa.depchain.proto.CommitmentMessage;
import pt.ulisboa.depchain.proto.ConsensusMessageType;
import pt.ulisboa.depchain.proto.Message;
import pt.ulisboa.depchain.proto.Node;
import pt.ulisboa.depchain.proto.ThresholdContextMessage;
import pt.ulisboa.depchain.proto.ThresholdSignatureContext;
import pt.ulisboa.depchain.server.consensus.ConsensusTimeoutException;
import pt.ulisboa.depchain.server.consensus.hotstuff.HotStuffSupport;
import pt.ulisboa.depchain.server.consensus.network.ReplicaTransportIds;
import pt.ulisboa.depchain.server.consensus.network.SerializedPeerSender;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.utils.ProtoValidationUtil;
import pt.ulisboa.depchain.shared.utils.QuorumAccumulator;
import pt.ulisboa.depchain.shared.utils.TimeUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class ThresholdSignatureExchange {
  private static final Logger logger = LoggerFactory.getLogger(ThresholdSignatureExchange.class);
  private static final Boolean MATCHING_GROUP = Boolean.TRUE;

  record ThresholdContext(byte[] aggregatedCommitment, Set<Integer> participantIndexes) {
  }

  record RemoteCommitment(int senderId, int participantIndex, byte[] commitment) {
  }

  private final ConfigParser config;
  private final int localSenderId;
  private final long thresholdRoundTimeoutMs;
  private final Map<Integer, InetSocketAddress> consensusEndpointsBySenderId;
  private final SerializedPeerSender peerSendScheduler;

  private AuthenticatedLink nodeTransport;

  ThresholdSignatureExchange(ConfigParser config, int localSenderId, long thresholdRoundTimeoutMs) {
    ValidationUtils.requireAllNonNull(named("config", config));
    ValidationUtils.requireNonNegativeInt(localSenderId, "localSenderId");
    ValidationUtils.requireNonNegativeLong(thresholdRoundTimeoutMs, "thresholdRoundTimeoutMs");

    this.config = config;
    this.localSenderId = localSenderId;
    this.thresholdRoundTimeoutMs = thresholdRoundTimeoutMs;
    this.consensusEndpointsBySenderId = ReplicaTransportIds.buildConsensusEndpointsBySenderId(config);
    this.peerSendScheduler = new SerializedPeerSender("threshold-send-" + localSenderId, logger);
  }

  void initTransport(AuthenticatedLink nodeTransport) {
    this.nodeTransport = ValidationUtils.requireNonNull(nodeTransport, "nodeTransport");
  }

  boolean isAuxiliaryMessage(Message msg) {
    return msg != null && HotStuffSupport.isAuxiliaryMessage(msg);
  }

  void sendCommitment(int leaderId, int viewNumber, int senderId, ConsensusMessageType type, Node node, byte[] commitment) throws Exception {
    ValidationUtils.requireNonNegativeInt(leaderId, "leaderId");
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");
    ValidationUtils.requireNonNegativeInt(senderId, "senderId");
    ValidationUtils.requireAllNonNull(named("type", type), named("node", node), named("commitment", commitment));

    Message message = Message.newBuilder().setViewNumber(viewNumber).setReplicaSenderId(senderId).setMessageType(type)
        .setCommitment(CommitmentMessage.newBuilder().setVotedNode(node).setThresholdSignatureCommitment(ByteString.copyFrom(commitment))).build();
    trySendMessage(leaderId, message);
  }

  ThresholdContext waitForContext(ConsensusMessageType type, int viewNumber, int leaderId, Node node, BlockingDeque<Message> messageQueue) {
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");
    ValidationUtils.requireNonNegativeInt(leaderId, "leaderId");
    ValidationUtils.requireAllNonNull(named("type", type), named("node", node), named("messageQueue", messageQueue));

    Message contextMessage = waitForMatchingMessage(messageQueue, msg -> msg.getReplicaSenderId() == leaderId
        && isMatchingThresholdMessage(msg, type, viewNumber, node, Message.BodyCase.THRESHOLD_CONTEXT));
    ThresholdSignatureContext context = contextMessage.getThresholdContext().getThresholdSignatureContext();
    Set<Integer> participantIndexes = new LinkedHashSet<>(context.getParticipantReplicaIndexesList());
    return new ThresholdContext(context.getAggregatedCommitment().toByteArray(), participantIndexes);
  }

  List<RemoteCommitment> collectCommitments(ConsensusMessageType type, int viewNumber, Node node, int minimumRemoteCount, BlockingDeque<Message> messageQueue, long deadlineNanos) {
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");
    ValidationUtils.requireNonNegativeInt(minimumRemoteCount, "minimumRemoteCount");
    ValidationUtils.requireAllNonNull(named("type", type), named("node", node), named("messageQueue", messageQueue));

    int maxRemoteCount = Math.max(0, config.system().n() - 1);
    QuorumAccumulator<Integer, Boolean, RemoteCommitment> commitmentsBySender = new QuorumAccumulator<>();
    ArrayDeque<Message> deferredMessages = new ArrayDeque<>();
    try {
      while (commitmentsBySender.count(MATCHING_GROUP) < minimumRemoteCount && !TimeUtil.hasTimedOutMonotonic(deadlineNanos)) {
        Message msg = pollMessageUntilDeadlineOrNull(messageQueue, deadlineNanos);
        if (msg == null) {
          break;
        }

        if (isMatchingThresholdMessage(msg, type, viewNumber, node, Message.BodyCase.COMMITMENT)) {
          commitmentsBySender.record(msg.getReplicaSenderId(), MATCHING_GROUP, new RemoteCommitment(msg.getReplicaSenderId(), replicaIndexForSender(msg.getReplicaSenderId()),
              msg.getCommitment().getThresholdSignatureCommitment().toByteArray()));
        } else {
          deferredMessages.addLast(msg);
        }
      }

      while (commitmentsBySender.count(MATCHING_GROUP) < maxRemoteCount) {
        Message msg = messageQueue.poll();
        if (msg == null) {
          break;
        }

        if (isMatchingThresholdMessage(msg, type, viewNumber, node, Message.BodyCase.COMMITMENT)) {
          commitmentsBySender.record(msg.getReplicaSenderId(), MATCHING_GROUP, new RemoteCommitment(msg.getReplicaSenderId(), replicaIndexForSender(msg.getReplicaSenderId()),
              msg.getCommitment().getThresholdSignatureCommitment().toByteArray()));
        } else {
          deferredMessages.addLast(msg);
        }
      }
    } finally {
      restoreDeferredMessages(messageQueue, deferredMessages);
    }

    if (!commitmentsBySender.hasCount(MATCHING_GROUP, minimumRemoteCount)) {
      throw new ConsensusTimeoutException("Timed out waiting for threshold commitments");
    }

    return commitmentsBySender.values(MATCHING_GROUP);
  }

  void sendContextMessages(Set<Integer> participantSenders, int viewNumber, int senderId, ConsensusMessageType type, Node node, byte[] aggregatedCommitment, int[] participantIndexes)
      throws Exception {
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");
    ValidationUtils.requireNonNegativeInt(senderId, "senderId");
    ValidationUtils
        .requireAllNonNull(named("participantSenders", participantSenders), named("type", type), named("node", node), named("aggregatedCommitment", aggregatedCommitment), named("participantIndexes", participantIndexes));

    for (Integer participantSenderId : participantSenders) {
      ThresholdSignatureContext context = ThresholdSignatureContext.newBuilder().setAggregatedCommitment(ByteString.copyFrom(aggregatedCommitment))
          .addAllParticipantReplicaIndexes(Arrays.stream(participantIndexes).boxed().toList()).build();
      Message message = Message.newBuilder().setViewNumber(viewNumber).setReplicaSenderId(senderId).setMessageType(type)
          .setThresholdContext(ThresholdContextMessage.newBuilder().setVotedNode(node).setThresholdSignatureContext(context)).build();
      trySendMessage(participantSenderId, message);
    }
  }

  List<byte[]> collectSignatureShares(ConsensusMessageType type, int viewNumber, Node node, byte[] aggregatedCommitment, Set<Integer> expectedSenders, BlockingDeque<Message> messageQueue, long deadlineNanos) {
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");
    ValidationUtils
        .requireAllNonNull(named("type", type), named("node", node), named("aggregatedCommitment", aggregatedCommitment), named("expectedSenders", expectedSenders), named("messageQueue", messageQueue));

    if (expectedSenders.isEmpty()) {
      return List.of();
    }

    List<Message> signatureMessages = collectMatchingMessages(expectedSenders
        .size(), messageQueue, msg -> isMatchingThresholdMessage(msg, type, viewNumber, node, Message.BodyCase.VOTE) && expectedSenders.contains(msg.getReplicaSenderId())
            && msg.getVote().getAggregatedCommitment().toByteArray().length > 0
            && Arrays.equals(msg.getVote().getAggregatedCommitment().toByteArray(), aggregatedCommitment), deadlineNanos);

    List<byte[]> signatures = new ArrayList<>(signatureMessages.size());
    for (Message signatureMessage : signatureMessages) {
      signatures.add(signatureMessage.getVote().getThresholdSignatureShare().toByteArray());
    }

    return signatures;
  }

  private void sendMessage(int senderId, Message msg) throws Exception {
    ValidationUtils.requireNonNegativeInt(senderId, "senderId");
    ValidationUtils.requireAllNonNull(named("msg", msg), named("nodeTransport", nodeTransport));

    byte[] payload = ProtoValidationUtil.requireValid(msg, "ReplicaMessage").toByteArray();
    nodeTransport.send(thresholdConnectionId(senderId, msg.getViewNumber()), payload, requireConsensusEndpoint(senderId));
  }

  private void trySendMessage(int senderId, Message msg) {
    byte[] payload;
    try {
      payload = ProtoValidationUtil.requireValid(msg, "ReplicaMessage").toByteArray();
    } catch (RuntimeException exception) {
      logger.debug("Ignoring invalid threshold message for replica {} at view {}", senderId, msg.getViewNumber(), exception);
      return;
    }

    peerSendScheduler.schedule(senderId, "threshold transport send for " + msg.getMessageType() + " at view " + msg.getViewNumber(), () -> {
      if (nodeTransport == null) {
        return;
      }
      nodeTransport.send(thresholdConnectionId(senderId, msg.getViewNumber()), payload, requireConsensusEndpoint(senderId));
    });
  }

  private List<Message> collectMatchingMessages(int expectedCount, BlockingDeque<Message> messageQueue, Predicate<Message> matcher, long deadlineNanos) {
    ValidationUtils.requireNonNegativeInt(expectedCount, "expectedCount");
    ValidationUtils.requireAllNonNull(named("messageQueue", messageQueue), named("matcher", matcher));

    QuorumAccumulator<Integer, Boolean, Message> messagesBySender = new QuorumAccumulator<>();
    ArrayDeque<Message> deferredMessages = new ArrayDeque<>();
    try {
      while (true) {
        Message msg = takeMessageUntilDeadline(messageQueue, deadlineNanos);
        if (matcher.test(msg)) {
          List<Message> matchingMessages = messagesBySender.recordAndGetValuesIfQuorumReached(msg.getReplicaSenderId(), MATCHING_GROUP, msg, expectedCount);
          if (!matchingMessages.isEmpty()) {
            return matchingMessages;
          }
        } else {
          deferredMessages.addLast(msg);
        }
      }
    } finally {
      restoreDeferredMessages(messageQueue, deferredMessages);
    }
  }

  private Message waitForMatchingMessage(BlockingDeque<Message> messageQueue, Predicate<Message> matcher) {
    ValidationUtils.requireAllNonNull(named("messageQueue", messageQueue), named("matcher", matcher));

    long deadlineNanos = TimeUtil.monotonicDeadlineAfterNow(thresholdRoundTimeoutMs);
    ArrayDeque<Message> deferredMessages = new ArrayDeque<>();
    try {
      while (true) {
        Message msg = takeMessageUntilDeadline(messageQueue, deadlineNanos);
        if (matcher.test(msg)) {
          return msg;
        }

        deferredMessages.addLast(msg);
      }
    } finally {
      restoreDeferredMessages(messageQueue, deferredMessages);
    }
  }

  private Message pollMessageUntilDeadlineOrNull(BlockingDeque<Message> messageQueue, long deadlineNanos) {
    try {
      long remainingMs = TimeUtil.monotonicRemainingMsUntil(deadlineNanos);
      if (remainingMs <= 0) {
        return null;
      }
      return messageQueue.poll(remainingMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new ConsensusTimeoutException("Interrupted while waiting for threshold messages");
    }
  }

  private Message takeMessageUntilDeadline(BlockingDeque<Message> messageQueue, long deadlineNanos) {
    if (TimeUtil.hasTimedOutMonotonic(deadlineNanos)) {
      throw new ConsensusTimeoutException("Timed out waiting for threshold messages");
    }

    try {
      long remainingMs = TimeUtil.monotonicRemainingMsUntil(deadlineNanos);
      Message msg = messageQueue.poll(remainingMs, TimeUnit.MILLISECONDS);
      if (msg == null) {
        throw new ConsensusTimeoutException("Timed out waiting for threshold messages");
      }
      return msg;
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new ConsensusTimeoutException("Interrupted while waiting for threshold messages");
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

  private InetSocketAddress requireConsensusEndpoint(int senderId) {
    InetSocketAddress endpoint = consensusEndpointsBySenderId.get(senderId);
    if (endpoint == null) {
      throw new IllegalArgumentException("Unknown consensus endpoint for senderId " + senderId);
    }
    return endpoint;
  }

  private static void restoreDeferredMessages(BlockingDeque<Message> messageQueue, ArrayDeque<Message> deferredMessages) {
    while (!deferredMessages.isEmpty()) {
      messageQueue.addFirst(deferredMessages.removeLast());
    }
  }

  private long thresholdConnectionId(int remoteSenderId, int messageViewNumber) {
    return ReplicaTransportIds.connectionIdForView(messageViewNumber, localSenderId, remoteSenderId, ReplicaTransportIds.THRESHOLD_MESSAGE_LANE);
  }
}
