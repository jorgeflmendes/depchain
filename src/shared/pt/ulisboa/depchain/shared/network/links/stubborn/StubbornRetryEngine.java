package pt.ulisboa.depchain.shared.network.links.stubborn;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import pt.ulisboa.depchain.shared.network.links.LinkFailureException;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedMessage;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedTargetKey;
import pt.ulisboa.depchain.shared.utils.TimeUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class StubbornRetryEngine {
  private record PendingRetry(byte[] payload, TrackedTargetKey target) {
  }

  private final StubbornContext context;
  private final StubbornSender sender;

  StubbornRetryEngine(StubbornContext context, StubbornSender sender) {
    ValidationUtils.requireAllNonNull(named("context", context), named("sender", sender));
    this.context = context;
    this.sender = sender;
  }

  void runRetryLoop() {
    java.util.Collection<TrackedMessage> remainingTracked = java.util.List.of();
    try {
      while (context.running.get()) {
        try {
          PendingRetry pendingRetry = awaitPendingRetry();
          if (pendingRetry != null) { // The sender is still running and a pending retry is due, so send it.
            sender.sendBestEffort(pendingRetry.payload(), pendingRetry.target().endpoint(), pendingRetry.target().key());
          }
        } catch (InterruptedException ignored) {
          if (!context.running.get()) {
            break;
          }
        }
      }
    } finally {
      synchronized (context.stateLock) {
        remainingTracked = context.retryRegistry.takeAllTrackedMessages();
        context.retryRegistry.clear();
      }
      for (TrackedMessage trackedMessage : remainingTracked) {
        trackedMessage.notifyTerminalState();
      }
    }
  }

  // Waits for the next pending retry to be due and returns it, or returns null if the sender is
  // closed.
  private PendingRetry awaitPendingRetry() throws InterruptedException {
    while (true) {
      TrackedMessage terminalTracked = null;
      boolean closed = false;

      synchronized (context.stateLock) {
        while (context.running.get()) {
          long now = TimeUtil.nowMs();

          RetryRegistry.ScheduledRetry scheduledRetry = context.retryRegistry.peekScheduledRetry();
          if (scheduledRetry == null) { // No pending retries, so wait again.
            context.stateLock.wait();
            continue;
          }

          long waitMs = TimeUtil.remainingMsUntil(scheduledRetry.dueAtMs(), now);
          if (waitMs > 0L) { // The next retry is scheduled for the future, so wait till then.
            context.stateLock.wait(waitMs);
            continue;
          }

          context.retryRegistry.pollScheduledRetry();
          TrackedTargetKey targetKey = scheduledRetry.target();
          TrackedMessage tracked = context.retryRegistry.getTrackedMessage(targetKey);
          if (tracked == null || tracked.nextRetryAtMs() != scheduledRetry.dueAtMs()) { // The tracked message was removed or rescheduled, so skip.
            continue;
          }

          if (context.shouldStopTracking(tracked)) { // The tracked message has reached the maximum retry attempts, so stop tracking and skip.
            terminalTracked = context.retryRegistry.removeTrackedMessage(targetKey);
            context.retryRegistry.recordTerminalFailure(targetKey, new LinkFailureException("Tracked message failed after max retries: " + targetKey));
            break;
          }
          // Update the tracked message with the new retry attempt and next retry time.
          tracked.markRetried(TimeUtil.deadlineAfter(now, context.computeRetryDelayMs(tracked.retryAttempt() + 1)));
          context.retryRegistry.putTrackedMessage(targetKey, tracked);
          return new PendingRetry(tracked.payloadView(), targetKey);
        }

        closed = !context.running.get();
      }

      if (closed) {
        return null;
      }
      if (terminalTracked != null) {
        terminalTracked.notifyTerminalState();
      }
    }
  }
}
