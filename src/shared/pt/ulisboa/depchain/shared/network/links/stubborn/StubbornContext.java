package pt.ulisboa.depchain.shared.network.links.stubborn;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import pt.ulisboa.depchain.shared.network.links.fairloss.FairLossLink;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedMessage;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class StubbornContext {
  final AtomicBoolean running;
  final FairLossLink fairLossLink;
  final RetryRegistry retryRegistry;
  final Object stateLock;

  StubbornContext(FairLossLink fairLossLink) {
    ValidationUtils.requireAllNonNull(named("fairLoss", fairLossLink));
    this.fairLossLink = fairLossLink;
    this.retryRegistry = new RetryRegistry();
    this.stateLock = new Object();
    this.running = new AtomicBoolean(true);
  }

  void ensureOpen() {
    if (!running.get()) {
      throw new IllegalStateException("StubbornLink is closed");
    }
  }

  long computeRetryDelayMs(int attempt) {
    int checkedAttempt = ValidationUtils.requireNonNegativeInt(attempt, "attempt");
    long maxDelayMs = StubbornLink.DEFAULT_MAX_DELAY_MS;
    long exponentialDelayMs = StubbornLink.DEFAULT_BASE_DELAY_MS * (1L << checkedAttempt);
    long boundedDelayMs = Math.min(maxDelayMs, exponentialDelayMs);

    double jitterScale = 1 + ThreadLocalRandom.current().nextDouble(-StubbornLink.DEFAULT_JITTER_RATIO, StubbornLink.DEFAULT_JITTER_RATIO);
    long jitteredDelayMs = Math.round(boundedDelayMs * jitterScale);

    return Math.max(1L, Math.min(maxDelayMs, jitteredDelayMs));
  }

  boolean shouldStopTracking(TrackedMessage tracked) {
    ValidationUtils.requireNonNull(tracked, "tracked");
    return tracked.retryAttempt() >= StubbornLink.DEFAULT_MAX_RETRY_ATTEMPTS;
  }
}

