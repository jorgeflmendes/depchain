package pt.ulisboa.depchain.shared.network.links.stubborn;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import pt.ulisboa.depchain.shared.network.links.LinkFailureException;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedMessage;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedTargetKey;
import pt.ulisboa.depchain.shared.utils.TimeUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class RetryRegistry {
  record ScheduledRetry(TrackedTargetKey target, long dueAtMs) {
    ScheduledRetry {
      ValidationUtils.requireNonNegativeLong(dueAtMs, "dueAtMs");
    }
  }

  // To order scheduled retries by their due time.
  private static final Comparator<ScheduledRetry> BY_DUE_AT = Comparator.comparingLong(ScheduledRetry::dueAtMs);

  private final long pendingCancelTtlMs;

  private final Map<TrackedTargetKey, TrackedMessage> tracked = new HashMap<>();
  private final Map<TrackedTargetKey, LinkFailureException> failedTracked = new HashMap<>();
  private final Map<TrackedTargetKey, Long> pendingCancels = new HashMap<>();
  private final PriorityQueue<ScheduledRetry> retryHeap = new PriorityQueue<>(BY_DUE_AT);

  public RetryRegistry(long pendingCancelTtlMs) {
    this.pendingCancelTtlMs = pendingCancelTtlMs;
  }

  public ScheduledRetry peekScheduled() {
    return retryHeap.peek();
  }

  public void pollScheduled() {
    retryHeap.poll();
  }

  public void putTracked(TrackedTargetKey key, TrackedMessage message) {
    tracked.put(key, message);
    retryHeap.offer(new ScheduledRetry(key, message.nextRetryAtMs()));
  }

  public TrackedMessage getTracked(TrackedTargetKey key) {
    return tracked.get(key);
  }

  public TrackedMessage removeTracked(TrackedTargetKey key) {
    failedTracked.remove(key);
    return tracked.remove(key);
  }

  public void recordFailed(TrackedTargetKey key, LinkFailureException failure) {
    failedTracked.put(key, failure);
  }

  public LinkFailureException trackedFailureOrNull(TrackedTargetKey key) {
    return failedTracked.get(key);
  }

  public boolean shouldSkipTrackedRegistration(TrackedTargetKey key, long now) {
    Long cancelAtMs = pendingCancels.remove(key);
    return cancelAtMs != null && !TimeUtil.hasElapsedMoreThan(now, cancelAtMs, pendingCancelTtlMs); // If there is a pending cancel that has not expired.
  }

  public void recordPendingCancel(TrackedTargetKey key, long now) {
    pendingCancels.put(key, now);
  }

  public void prunePendingCancels(long now) {
    if (!pendingCancels.isEmpty()) {
      pendingCancels.entrySet().removeIf(entry -> TimeUtil.hasElapsedAtLeast(now, entry.getValue(), pendingCancelTtlMs)); // Remove pending cancels that have expired.
    }
  }

  public void clear() {
    tracked.clear();
    failedTracked.clear();
    pendingCancels.clear();
    retryHeap.clear();
  }
}
