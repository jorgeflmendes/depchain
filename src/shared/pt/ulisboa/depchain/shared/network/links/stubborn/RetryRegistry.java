package pt.ulisboa.depchain.shared.network.links.stubborn;

import java.util.Comparator;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import pt.ulisboa.depchain.shared.network.links.LinkFailureException;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedMessage;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedTargetKey;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class RetryRegistry {
  record ScheduledRetry(TrackedTargetKey target, long dueAtMs) {
    ScheduledRetry {
      ValidationUtils.requireNonNegativeLong(dueAtMs, "dueAtMs");
    }
  }

  // To order scheduled retries by their due time.
  private static final Comparator<ScheduledRetry> BY_DUE_AT = Comparator.comparingLong(ScheduledRetry::dueAtMs);

  private final Map<TrackedTargetKey, TrackedMessage> trackedMessagesByTarget = new HashMap<>();
  private final Map<TrackedTargetKey, LinkFailureException> terminalFailuresByTarget = new HashMap<>();
  private final PriorityQueue<ScheduledRetry> scheduledRetries = new PriorityQueue<>(BY_DUE_AT);

  public ScheduledRetry peekScheduledRetry() {
    return scheduledRetries.peek();
  }

  public void pollScheduledRetry() {
    scheduledRetries.poll();
  }

  public void putTrackedMessage(TrackedTargetKey key, TrackedMessage message) {
    trackedMessagesByTarget.put(key, message);
    scheduledRetries.offer(new ScheduledRetry(key, message.nextRetryAtMs()));
  }

  public TrackedMessage getTrackedMessage(TrackedTargetKey key) {
    return trackedMessagesByTarget.get(key);
  }

  public TrackedMessage removeTrackedMessage(TrackedTargetKey key) {
    terminalFailuresByTarget.remove(key);
    return trackedMessagesByTarget.remove(key);
  }

  public void recordTerminalFailure(TrackedTargetKey key, LinkFailureException failure) {
    terminalFailuresByTarget.put(key, failure);
  }

  public LinkFailureException pollTerminalFailure(TrackedTargetKey key) {
    return terminalFailuresByTarget.remove(key);
  }

  public Collection<TrackedMessage> takeAllTrackedMessages() {
    Collection<TrackedMessage> trackedMessages = java.util.List.copyOf(trackedMessagesByTarget.values());
    trackedMessagesByTarget.clear();
    scheduledRetries.clear();
    return trackedMessages;
  }

  public void clear() {
    trackedMessagesByTarget.clear();
    terminalFailuresByTarget.clear();
    scheduledRetries.clear();
  }
}

