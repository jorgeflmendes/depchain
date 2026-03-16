package pt.ulisboa.depchain.server.consensus;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import pt.ulisboa.depchain.proto.ConsensusMessageType;
import pt.ulisboa.depchain.proto.Message;
import pt.ulisboa.depchain.proto.QuorumCertificate;
import pt.ulisboa.depchain.server.consensus.threshold.ThresholdSignatureProtocol;
import pt.ulisboa.depchain.shared.utils.TimeUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class HotStuffMessageInbox {
  private final int localSenderId;
  private final BlockingDeque<Message> messageQueue;
  private final ThresholdSignatureProtocol thresholdProtocol;

  HotStuffMessageInbox(int localSenderId, ThresholdSignatureProtocol thresholdProtocol) {
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
    LinkedHashMap<Integer, Message> messagesBySender = new LinkedHashMap<>();
    ArrayDeque<Message> deferredMessages = new ArrayDeque<>();
    try {
      while (messagesBySender.size() < requiredCount) {
        Message message = pollMessageUntil(deadlineNanos, "waiting for " + type + " messages");
        if (matchingMessage(message, type, view) && !thresholdProtocol.isAuxiliaryMessage(message)) {
          messagesBySender.putIfAbsent(message.getReplicaSenderId(), message);
        } else {
          deferredMessages.addLast(message);
        }
      }
      return new ArrayList<>(messagesBySender.values());
    } finally {
      restoreDeferredMessages(deferredMessages);
    }
  }

  List<Message> waitForValidNewViewsUntil(int view, int requiredRemoteCount, long deadlineNanos, Predicate<QuorumCertificate> qcVerifier) {
    LinkedHashMap<Integer, Message> messagesBySender = new LinkedHashMap<>();
    ArrayDeque<Message> deferredMessages = new ArrayDeque<>();
    try {
      while (messagesBySender.size() < requiredRemoteCount) {
        Message message = pollMessageUntil(deadlineNanos, "waiting for valid NEW_VIEW messages");
        if (isValidRemoteNewView(message, view, qcVerifier)) {
          messagesBySender.putIfAbsent(message.getReplicaSenderId(), message);
        } else {
          deferredMessages.addLast(message);
        }
      }
      return new ArrayList<>(messagesBySender.values());
    } finally {
      restoreDeferredMessages(deferredMessages);
    }
  }

  Message waitForMessageFromSender(ConsensusMessageType type, int view, int senderId, long timeoutMs) {
    long deadlineNanos = TimeUtil.monotonicDeadlineAfterNow(timeoutMs);
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

  Message waitForQcMessage(ConsensusMessageType type, int view, int senderId, long timeoutMs, Predicate<QuorumCertificate> qcVerifier) {
    long deadlineNanos = TimeUtil.monotonicDeadlineAfterNow(timeoutMs);
    ArrayDeque<Message> deferredMessages = new ArrayDeque<>();
    try {
      while (true) {
        Message message = pollMessageUntil(deadlineNanos, "waiting for QC " + type);
        QuorumCertificate justifyQc = message.hasPhaseCertificate() ? message.getPhaseCertificate().getJustifyQc() : null;
        if (matchingQc(justifyQc, type, view) && message.getReplicaSenderId() == senderId && !thresholdProtocol.isAuxiliaryMessage(message)
            && qcVerifier.test(justifyQc)) {
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
      throw new ViewChangeTimeoutException("Timed out " + description);
    }

    try {
      long remainingMs = TimeUtil.monotonicRemainingMsUntil(deadlineNanos);
      Message message = messageQueue.poll(remainingMs, TimeUnit.MILLISECONDS);
      if (message == null) {
        throw new ViewChangeTimeoutException("Timed out " + description);
      }
      return message;
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new ViewChangeTimeoutException("Interrupted while " + description);
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
