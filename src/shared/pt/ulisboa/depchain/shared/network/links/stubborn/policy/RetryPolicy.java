package pt.ulisboa.depchain.shared.network.links.stubborn.policy;

import java.util.concurrent.ThreadLocalRandom;

import pt.ulisboa.depchain.shared.network.links.stubborn.StubbornLink;
import pt.ulisboa.depchain.shared.network.links.stubborn.model.TrackedMessage;

// Retry Logic for StubbornLink.
public final class RetryPolicy {
  private final long baseDelayMs;
  private final long maxDelayMs;
  private final double jitterRatio;
  private final int maxRetryAttempts;
  private final long maxTrackedLifetimeMs;

  public RetryPolicy(long baseDelayMs, long maxDelayMs, double jitterRatio, int maxRetryAttempts, long maxTrackedLifetimeMs) {
    this.baseDelayMs = baseDelayMs;
    this.maxDelayMs = maxDelayMs;
    this.jitterRatio = jitterRatio;
    this.maxRetryAttempts = maxRetryAttempts;
    this.maxTrackedLifetimeMs = maxTrackedLifetimeMs;
  }

  public long baseDelayMs() {
    return baseDelayMs;
  }

  public long maxDelayMs() {
    return maxDelayMs;
  }

  public long maxTrackedLifetimeMs() {
    return maxTrackedLifetimeMs;
  }

  public long pendingCancelTtlMs() {
    return pendingCancelTtlMs(maxDelayMs, maxTrackedLifetimeMs);
  }

  // Computes the delay for the next retry attempt using exponential backoff with jitter.
  public long computeDelayMs(int attempt) {
    long delay = baseDelayMs;
    for (int i = 0; i < attempt && delay < maxDelayMs; i++) {
      long doubled;
      if (delay > (Long.MAX_VALUE >>> 1)) {
        doubled = Long.MAX_VALUE;
      } else {
        doubled = (delay << 1);
      }
      delay = Math.min(doubled, maxDelayMs);
    }

    // Jitter is used to add randomness to the retry delay to avoid thundering herd problems.
    double jitter;
    if (jitterRatio == 0.0d) {
      jitter = 0.0d;
    } else {
      jitter = ThreadLocalRandom.current().nextDouble(-jitterRatio, jitterRatio);
    }

    return Math.max(1L, Math.round(delay * (1.0d + jitter)));
  }

  public boolean shouldStopTracking(TrackedMessage tracked, long now) {
    if (hasRetryAttemptsLimit() && tracked.retryAttempt() >= maxRetryAttempts) { // Stop tracking if retry attempts limit is reached
      return true;
    }
    
    return !isUnlimitedTracked(maxTrackedLifetimeMs) && (now - tracked.createdAtMs()) >= maxTrackedLifetimeMs; // Stop tracking if tracked lifetime exceeded
  }

  private static long pendingCancelTtlMs(long maxDelayMs, long maxTrackedLifetimeMs) {
    if (isUnlimitedTracked(maxTrackedLifetimeMs)) { // If the tracked lifetime is unlimited, use the max delay as the TTL for pending cancels.
      return maxDelayMs;
    }

    return maxTrackedLifetimeMs;
  }

  private boolean hasRetryAttemptsLimit() {
    return maxRetryAttempts != StubbornLink.Config.UNLIMITED_RETRY_ATTEMPTS;
  }

  private static boolean isUnlimitedTracked(long value) {
    return value == StubbornLink.Config.UNLIMITED_TRACKED_LIFETIME_MS;
  }
}
