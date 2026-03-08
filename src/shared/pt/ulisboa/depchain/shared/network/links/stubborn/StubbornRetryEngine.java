package pt.ulisboa.depchain.shared.network.links.stubborn;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

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
        } catch (RuntimeException exception) {
          System.out.printf("Stubborn retry loop error = %s%n", exception.getMessage());
        }
      }
    } finally {
      synchronized (context.stateLock) {
        context.retryRegistry.clear();
      }
    }
  }

  // Waits for the next pending retry to be due and returns it, or returns null if the sender is
  // closed.
  private PendingRetry awaitPendingRetry() throws InterruptedException {
    synchronized (context.stateLock) {
      while (context.running.get()) {
        long now = TimeUtil.nowMs();
        context.retryRegistry.prunePendingCancels(now);

        RetryRegistry.ScheduledRetry scheduledRetry = context.retryRegistry.peekScheduled();
        if (scheduledRetry == null) { // No pending retries, so wait again.
          context.stateLock.wait();
          continue;
        }

        long waitMs = TimeUtil.remainingMsUntil(scheduledRetry.dueAtMs(), now);
        if (waitMs > 0L) { // The next retry is scheduled for the future, so wait till then.
          context.stateLock.wait(waitMs);
          continue;
        }

        context.retryRegistry.pollScheduled();
        TrackedTargetKey targetKey = scheduledRetry.target();
        TrackedMessage tracked = context.retryRegistry.getTracked(targetKey);
        if (tracked == null || tracked.nextRetryAtMs() != scheduledRetry.dueAtMs()) { // The tracked message was removed or rescheduled, so skip.
          continue;
        }

        if (context.shouldStopTracking(tracked, now)) { // The tracked message has reached the maximum retry attempts, so stop tracking and skip.
          context.retryRegistry.removeTracked(targetKey);
          continue;
        }

        tracked.markRetried(TimeUtil.deadlineAfter(now, context.computeRetryDelayMs(tracked.retryAttempt() + 1))); // Update the tracked message with the new retry attempt and next
                                                                                                                   // retry time.
        context.retryRegistry.putTracked(targetKey, tracked);
        return new PendingRetry(tracked.payload(), targetKey);
      }

      return null;
    }
  }
}
