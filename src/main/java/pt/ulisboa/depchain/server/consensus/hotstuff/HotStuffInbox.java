package pt.ulisboa.depchain.server.consensus.hotstuff;

import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import pt.ulisboa.depchain.proto.ConsensusMessageType;
import pt.ulisboa.depchain.proto.Message;
import pt.ulisboa.depchain.proto.QuorumCertificate;
import pt.ulisboa.depchain.server.consensus.ConsensusTimeoutException;
import pt.ulisboa.depchain.server.consensus.threshold.ThresholdSignatureProtocol;
import pt.ulisboa.depchain.shared.quorum.QuorumAccumulator;
import pt.ulisboa.depchain.shared.time.TimeUtil;
import pt.ulisboa.depchain.shared.validation.ValidationUtils;

final class HotStuffInbox {
  private static final Boolean MATCHING_GROUP = Boolean.TRUE;

  private final int localSenderId;
  private final BlockingDeque<Message> messageQueue;
  private final ThresholdSignatureProtocol thresholdProtocol;

  HotStuffInbox(int localSenderId, ThresholdSignatureProtocol thresholdProtocol) {
    this.localSenderId = localSenderId;
    this.messageQueue = new LinkedBlockingDeque<>();
    this.thresholdProtocol = ValidationUtils.requireNonNull(thresholdProtocol, "thresholdProtocol");
  }

  BlockingDeque<Message> sharedQueueForThreshold() {
    return messageQueue;
  }

  void offer(Message message) {
    messageQueue.add(message);
  }

  List<Message> waitForQuorumMessagesUntil(ConsensusMessageType type, int view, int requiredCount, long deadlineNanos) {
    QuorumAccumulator<Integer, Boolean, Message> messagesBySender = new QuorumAccumulator<>();
    ArrayDeque<Message> deferredMessages = new ArrayDeque<>();
    try {
      while (true) {
        Message message = pollMessageUntil(deadlineNanos, "waiting for " + type + " messages");
        if (matchingMessage(message, type, view) && !thresholdProtocol.isAuxiliaryMessage(message)) {
          List<Message> quorumMessages = messagesBySender.recordAndGetValuesIfQuorumReached(message.getReplicaSenderId(), MATCHING_GROUP, message, requiredCount);
          if (!quorumMessages.isEmpty()) {
            return quorumMessages;
          }
        } else {
          deferredMessages.addLast(message);
        }
      }
    } finally {
      restoreDeferredMessages(deferredMessages);
    }
  }

  List<Message> waitForValidNewViewsUntil(int view, int requiredRemoteCount, long deadlineNanos, Predicate<QuorumCertificate> qcVerifier) {
    QuorumAccumulator<Integer, Boolean, Message> messagesBySender = new QuorumAccumulator<>();
    ArrayDeque<Message> deferredMessages = new ArrayDeque<>();
    try {
      while (true) {
        Message message = pollMessageUntil(deadlineNanos, "waiting for valid NEW_VIEW messages");
        if (isValidRemoteNewView(message, view, qcVerifier)) {
          List<Message> quorumMessages = messagesBySender.recordAndGetValuesIfQuorumReached(message.getReplicaSenderId(), MATCHING_GROUP, message, requiredRemoteCount);
          if (!quorumMessages.isEmpty()) {
            return quorumMessages;
          }
        } else {
          deferredMessages.addLast(message);
        }
      }
    } finally {
      restoreDeferredMessages(deferredMessages);
    }
  }

  Message waitForMessageFromSenderUntil(ConsensusMessageType type, int view, int senderId, long deadlineNanos) {
    ArrayDeque<Message> deferredMessages = new ArrayDeque<>();
    try {
      while (true) {
        Message message = pollMessageUntil(deadlineNanos, "waiting for " + type + " from " + senderId);
        if (message.getReplicaSenderId() == senderId && matchingMessage(message, type, view) && !thresholdProtocol.isAuxiliaryMessage(message)) {
          return message;
        }
        deferredMessages.addLast(message);
      }
    } finally {
      restoreDeferredMessages(deferredMessages);
    }
  }

  Message waitForQcMessageUntil(ConsensusMessageType type, int view, int senderId, long deadlineNanos, Predicate<QuorumCertificate> qcVerifier) {
    ArrayDeque<Message> deferredMessages = new ArrayDeque<>();
    try {
      while (true) {
        Message message = pollMessageUntil(deadlineNanos, "waiting for QC " + type);
        QuorumCertificate justifyQc = null;
        if (message.hasPhaseCertificate()) {
          justifyQc = message.getPhaseCertificate().getJustifyQc();
        }
        if (matchingQc(justifyQc, type, view) && message.getReplicaSenderId() == senderId && !thresholdProtocol.isAuxiliaryMessage(message) && qcVerifier.test(justifyQc)) {
          return message;
        }
        deferredMessages.addLast(message);
      }
    } finally {
      restoreDeferredMessages(deferredMessages);
    }
  }

  Message pollMessageUntil(long deadlineNanos, String description) {
    if (TimeUtil.hasTimedOutMonotonic(deadlineNanos)) {
      throw new ConsensusTimeoutException("Timed out " + description);
    }

    try {
      long remainingMs = TimeUtil.monotonicRemainingMsUntil(deadlineNanos);
      Message message = messageQueue.poll(remainingMs, TimeUnit.MILLISECONDS);
      if (message == null) {
        throw new ConsensusTimeoutException("Timed out " + description);
      }
      return message;
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new ConsensusTimeoutException("Interrupted while " + description);
    }
  }

  private boolean isValidRemoteNewView(Message message, int expectedView, Predicate<QuorumCertificate> qcVerifier) {
    if (message == null || message.getReplicaSenderId() == localSenderId || thresholdProtocol.isAuxiliaryMessage(message)) {
      return false;
    }
    if (!matchingMessage(message, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_NEW_VIEW, expectedView) || !message.hasPhaseCertificate()) {
      return false;
    }

    QuorumCertificate justifyQc = message.getPhaseCertificate().getJustifyQc();
    return justifyQc != null && qcVerifier.test(justifyQc);
  }

  private boolean matchingMessage(Message message, ConsensusMessageType type, int view) {
    return message.getMessageType() == type && message.getViewNumber() == view;
  }

  private boolean matchingQc(QuorumCertificate qc, ConsensusMessageType type, int view) {
    return qc != null && qc.getMessageType() == type && qc.getViewNumber() == view;
  }

  void restoreDeferredMessages(ArrayDeque<Message> deferredMessages) {
    while (!deferredMessages.isEmpty()) {
      messageQueue.addFirst(deferredMessages.removeLast());
    }
  }
}
