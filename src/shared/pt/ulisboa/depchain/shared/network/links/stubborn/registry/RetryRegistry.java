package pt.ulisboa.depchain.shared.network.links.stubborn.registry;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;

import pt.ulisboa.depchain.shared.network.links.stubborn.model.ScheduledRetry;
import pt.ulisboa.depchain.shared.network.links.stubborn.model.TrackedEndpointKey;
import pt.ulisboa.depchain.shared.network.links.stubborn.model.TrackedMessage;

// Registry for tracking messages that are pending retry and pending cancel.
public final class RetryRegistry {
  // Comparator for the retry heap to order scheduled retries by their due time.
  private static final Comparator<ScheduledRetry> BY_DUE_AT = Comparator.comparingLong(ScheduledRetry::dueAtMs);

  private final int maxPending;
  private final int minRetryHeapSizeForCompaction;
  private final int maxPendingCancels;
  private final long pendingCancelTtlMs;

  private final Map<TrackedEndpointKey, TrackedMessage> tracked = new HashMap<>();
  private final Map<TrackedEndpointKey, Long> pendingCancels = new HashMap<>();
  private final PriorityQueue<ScheduledRetry> retryHeap = new PriorityQueue<>(BY_DUE_AT);

  public RetryRegistry(int maxPending, int minRetryHeapSizeForCompaction, int maxPendingCancels, long pendingCancelTtlMs) {
    this.maxPending = maxPending;
    this.minRetryHeapSizeForCompaction = minRetryHeapSizeForCompaction;
    this.maxPendingCancels = maxPendingCancels;
    this.pendingCancelTtlMs = pendingCancelTtlMs;
  }

  public ScheduledRetry peekScheduled() {
    return retryHeap.peek();
  }

  public void pollScheduled() {
    retryHeap.poll();
  }

  public void putTracked(TrackedEndpointKey key, TrackedMessage message) {
    tracked.put(key, message);
    retryHeap.offer(new ScheduledRetry(key.endpoint(), message.key(), message.nextRetryAtMs())); // Add a new scheduled retry for the tracked message to the retry heap.
    compactRetryHeapIfNeeded();
  }

  public TrackedMessage getTracked(TrackedEndpointKey key) {
    return tracked.get(key);
  }

  public TrackedMessage removeTracked(TrackedEndpointKey key) {
    return tracked.remove(key);
  }

  public boolean shouldSkipTrackedRegistration(TrackedEndpointKey key, long now) {
    Long cancelAtMs = pendingCancels.remove(key);
    return cancelAtMs != null && (now - cancelAtMs) <= pendingCancelTtlMs; // If there is a pending cancel that has not expired based on the TTL for pending cancels.
  }

  public boolean canRegisterTracked(TrackedEndpointKey key) {
    return tracked.containsKey(key) || tracked.size() < maxPending; // If there is already a tracked message for the key or if the registry is not at capacity.
  }

  public void recordPendingCancel(TrackedEndpointKey key, long now) {
    // If the pending cancels registry is at capacity, prune expired entries.
    if (pendingCancels.size() >= maxPendingCancels) { 
      prunePendingCancels(now);
    }

    // If still at capacity after pruning, remove an arbitrary entry.
    if (pendingCancels.size() >= maxPendingCancels) { 
      Iterator<TrackedEndpointKey> iterator = pendingCancels.keySet().iterator();
      if (iterator.hasNext()) {
        iterator.next();
        iterator.remove();
      }
    }
    
    pendingCancels.put(key, now);
  }

  public void prunePendingCancels(long now) {
    // Remove pending cancels that have expired based on their recorded timestamp and the configured TTL for pending cancels.
    if (!pendingCancels.isEmpty()) {
      pendingCancels.entrySet().removeIf(entry -> (now - entry.getValue()) >= pendingCancelTtlMs);
    }

    // If the size is now within limits after removing expired entries.
    if (pendingCancels.size() <= maxPendingCancels) { 
      return;
    }

    // While still above limits, remove arbitrary entries.
    Iterator<TrackedEndpointKey> iterator = pendingCancels.keySet().iterator();
    while (pendingCancels.size() > maxPendingCancels && iterator.hasNext()) {
      iterator.next();
      iterator.remove();
    }
  }

  // Rebuilds the heap if it has grown too large compared to the actively tracked messages.
  private void compactRetryHeapIfNeeded() {
    int heapSize = retryHeap.size();
    if (heapSize < minRetryHeapSizeForCompaction) {
      return;
    }

    int activeTrackedSize = tracked.size();
    if ((heapSize - activeTrackedSize) <= activeTrackedSize) {
      return;
    }

    PriorityQueue<ScheduledRetry> rebuilt = new PriorityQueue<>(BY_DUE_AT);
    for (Map.Entry<TrackedEndpointKey, TrackedMessage> entry : tracked.entrySet()) {
      TrackedEndpointKey endpointKey = entry.getKey();
      TrackedMessage message = entry.getValue();
      rebuilt.offer(new ScheduledRetry(endpointKey.endpoint(), message.key(), message.nextRetryAtMs()));
    }
    
    retryHeap.clear();
    retryHeap.addAll(rebuilt);
  }

  public void clear() {
    tracked.clear();
    pendingCancels.clear();
    retryHeap.clear();
  }
}
