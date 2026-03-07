package pt.ulisboa.depchain.shared.network.links.stubborn;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.util.concurrent.atomic.AtomicBoolean;

import pt.ulisboa.depchain.shared.network.links.fairloss.FairLossLink;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedMessage;
import pt.ulisboa.depchain.shared.utils.TimeUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class StubbornContext {
  final AtomicBoolean running;
  final FairLossLink fairLossLink;

  // Registry for tracking messages that are pending retry and pending cancel.
  final RetryRegistry retryRegistry;
  
  // Lock for sync access to shared state.
  final Object stateLock;
  
  StubbornContext(FairLossLink fairLossLink) {
    ValidationUtils.requireAllNonNull(named("fairLoss", fairLossLink));
    this.fairLossLink = fairLossLink;
    this.retryRegistry = new RetryRegistry(pendingCancelTtlMs());
    this.stateLock = new Object();
    this.running = new AtomicBoolean(true);
  }

  void ensureOpen() {
    if (!running.get()) {
      throw new IllegalStateException("StubbornLink is closed");
    }
  }

  long baseDelayMs() {
    return StubbornLink.DEFAULT_BASE_DELAY_MS;
  }

  long maxDelayMs() {
    return StubbornLink.DEFAULT_MAX_DELAY_MS;
  }

  long maxTrackedLifetimeMs() {
    return StubbornLink.DEFAULT_MAX_TRACKED_LIFETIME_MS;
  }

  // Exponential backoff with jitter, based on the attempt number.
  long computeRetryDelayMs(int attempt) {
    int checkedAttempt = ValidationUtils.requireNonNegativeInt(attempt, "attempt");
    long retryDelayMs = Math.min(maxDelayMs(), baseDelayMs() * (1L << checkedAttempt)); /// Exponential backoff delay.

    // Jitter 
    double jitterScale = 1 + (Math.random() * 2 - 1) * StubbornLink.DEFAULT_JITTER_RATIO;
    long jitteredDelay = Math.round(retryDelayMs * jitterScale);

    return Math.max(1L, Math.min(maxDelayMs(), jitteredDelay)); // Ensure the final delay is within bounds.
  }

  boolean shouldStopTracking(TrackedMessage tracked, long now) {
    ValidationUtils.requireNonNull(tracked, "tracked");
    ValidationUtils.requireNonNegativeLong(now, "now");

    boolean maxAttemptsReached = tracked.retryAttempt() >= StubbornLink.DEFAULT_MAX_RETRY_ATTEMPTS;
    boolean maxLifetimeExceeded =
        maxTrackedLifetimeMs() >= 0L
            && TimeUtil.hasElapsedAtLeast(now, tracked.createdAtMs(), maxTrackedLifetimeMs());

    return maxAttemptsReached || maxLifetimeExceeded;
  }

  private long pendingCancelTtlMs() {
    return Math.max(1L, maxDelayMs() + baseDelayMs());
  }
}
