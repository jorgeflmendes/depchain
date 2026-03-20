package pt.ulisboa.depchain.shared.network.links.stubborn;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.jdt.annotation.Nullable;

import pt.ulisboa.depchain.shared.network.links.LinkFailureException;
import pt.ulisboa.depchain.shared.network.links.fairloss.FairLossLink;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedMessage;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedTargetKey;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;

final class StubbornContext {
  final AtomicBoolean running;
  final FairLossLink fairLossLink;
  final HashedWheelTimer retryTimer;
  final Object retryLock;
  final Map<TrackedTargetKey, TrackedMessage> trackedMessagesByTarget;
  final Map<TrackedTargetKey, Timeout> retryTimeoutsByTarget;
  final Map<TrackedTargetKey, LinkFailureException> terminalFailuresByTarget;

  StubbornContext(FairLossLink fairLossLink) {
    this.fairLossLink = ValidationUtils.requireNonNull(fairLossLink, "fairLossLink");
    this.retryTimer = new HashedWheelTimer();
    this.retryLock = new Object();
    this.running = new AtomicBoolean(true);
    this.trackedMessagesByTarget = new HashMap<>();
    this.retryTimeoutsByTarget = new HashMap<>();
    this.terminalFailuresByTarget = new HashMap<>();
  }

  void ensureOpen() {
    if (!running.get()) {
      throw new IllegalStateException("StubbornLink is closed");
    }
  }

  long retryDelayMs(int attempt) {
    int checkedAttempt = ValidationUtils.requireNonNegativeInt(attempt, "attempt");
    long maxDelayMs = StubbornLink.DEFAULT_MAX_DELAY_MS;
    long exponentialDelayMs = StubbornLink.DEFAULT_BASE_DELAY_MS * (1L << checkedAttempt);
    long boundedDelayMs = Math.min(maxDelayMs, exponentialDelayMs);

    double jitterScale = 1 + ThreadLocalRandom.current().nextDouble(-StubbornLink.DEFAULT_JITTER_RATIO, StubbornLink.DEFAULT_JITTER_RATIO);
    long jitteredDelayMs = Math.round(boundedDelayMs * jitterScale);

    return Math.max(1L, Math.min(maxDelayMs, jitteredDelayMs));
  }

  boolean reachedRetryLimit(TrackedMessage tracked) {
    ValidationUtils.requireNonNull(tracked, "tracked");
    return tracked.retryAttempt() >= StubbornLink.DEFAULT_MAX_RETRY_ATTEMPTS;
  }

  void replaceRetryTimeout(TrackedTargetKey targetKey, Timeout retryTimeout) {
    Timeout previousTimeout = retryTimeoutsByTarget.put(targetKey, retryTimeout);
    if (previousTimeout != null) {
      previousTimeout.cancel();
    }
  }

  TrackedMessage removeTrackedMessage(TrackedTargetKey targetKey) {
    terminalFailuresByTarget.remove(targetKey);
    Timeout retryTimeout = retryTimeoutsByTarget.remove(targetKey);
    if (retryTimeout != null) {
      retryTimeout.cancel();
    }
    return trackedMessagesByTarget.remove(targetKey);
  }

  @Nullable
  LinkFailureException pollTerminalFailure(TrackedTargetKey targetKey) {
    return terminalFailuresByTarget.remove(targetKey);
  }
}
