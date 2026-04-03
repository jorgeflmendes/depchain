package pt.ulisboa.depchain.shared.network.links.stubborn;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.jdt.annotation.Nullable;

import pt.ulisboa.depchain.shared.network.links.LinkFailureException;
import pt.ulisboa.depchain.shared.network.links.fairloss.FairLossLink;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedKey;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedMessage;
import pt.ulisboa.depchain.shared.validation.ValidationUtils;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;

final class StubbornContext {
  static final long RETRY_SWEEP_MS = 10L;

  final AtomicBoolean running;
  final FairLossLink fairLossLink;
  final HashedWheelTimer retryTimer;
  final Object retryLock;
  final Map<InetSocketAddress, EndpointRetryState> retryStatesByEndpoint;
  Timeout retrySweepTimeout;

  StubbornContext(FairLossLink fairLossLink) {
    this.fairLossLink = ValidationUtils.requireNonNull(fairLossLink, "fairLossLink");
    this.retryTimer = new HashedWheelTimer();
    this.retryLock = new Object();
    this.running = new AtomicBoolean(true);
    this.retryStatesByEndpoint = new HashMap<>();
  }

  void ensureOpen() {
    if (!running.get()) {
      throw new IllegalStateException("StubbornLink is closed");
    }
  }

  long retryDelayMs(int attempt) {
    int checkedAttempt = ValidationUtils.requireNonNegativeInt(attempt, "attempt");
    if (checkedAttempt == 0) {
      return StubbornLink.DEFAULT_BASE_DELAY_MS;
    }

    long maxDelayMs = StubbornLink.DEFAULT_MAX_DELAY_MS;
    long boundedDelayMs = StubbornLink.DEFAULT_BASE_DELAY_MS;
    for (int i = 0; i < checkedAttempt && boundedDelayMs < maxDelayMs; i++) {
      boundedDelayMs = Math.min(maxDelayMs, boundedDelayMs * 2L);
    }
    long jitterRangeMs = Math.round(boundedDelayMs * StubbornLink.DEFAULT_JITTER_RATIO);
    if (jitterRangeMs == 0L) {
      return boundedDelayMs;
    }

    long lowerJitterBound = -jitterRangeMs;
    long upperJitterBoundExclusive = jitterRangeMs + 1L;
    long jitteredDelayMs = boundedDelayMs + ThreadLocalRandom.current().nextLong(lowerJitterBound, upperJitterBoundExclusive);
    return Math.max(1L, Math.min(maxDelayMs, jitteredDelayMs));
  }

  long retryDelayNanos(int attempt) {
    return TimeUnit.MILLISECONDS.toNanos(retryDelayMs(attempt));
  }

  void scheduleNextRetrySweep(StubbornSender sender) {
    if (!running.get()) {
      return;
    }
    retrySweepTimeout = retryTimer.newTimeout(ignored -> sender.runRetrySweep(), RETRY_SWEEP_MS, TimeUnit.MILLISECONDS);
  }

  void startRetrySweep(StubbornSender sender) {
    synchronized (retryLock) {
      scheduleNextRetrySweep(sender);
    }
  }

  void cancelRetrySweep() {
    if (retrySweepTimeout != null) {
      retrySweepTimeout.cancel();
      retrySweepTimeout = null;
    }
  }

  EndpointRetryState getOrCreateRetryState(InetSocketAddress remoteEndpoint) {
    return retryStatesByEndpoint.computeIfAbsent(remoteEndpoint, ignored -> new EndpointRetryState());
  }

  TrackedMessage removeTrackedMessage(InetSocketAddress remoteEndpoint, TrackedKey trackedKey) {
    EndpointRetryState retryState = retryStatesByEndpoint.get(remoteEndpoint);
    if (retryState == null) {
      return null;
    }

    retryState.terminalFailuresByKey.remove(trackedKey);
    TrackedMessage trackedMessage = retryState.trackedMessagesByKey.remove(trackedKey);
    if (retryState.isEmpty()) {
      retryStatesByEndpoint.remove(remoteEndpoint);
    }
    return trackedMessage;
  }

  void recordTerminalFailure(InetSocketAddress remoteEndpoint, TrackedKey trackedKey, LinkFailureException terminalFailure) {
    getOrCreateRetryState(remoteEndpoint).terminalFailuresByKey.put(trackedKey, terminalFailure);
  }

  @Nullable
  LinkFailureException pollTerminalFailure(InetSocketAddress remoteEndpoint, TrackedKey trackedKey) {
    EndpointRetryState retryState = retryStatesByEndpoint.get(remoteEndpoint);
    if (retryState == null) {
      return null;
    }

    LinkFailureException terminalFailure = retryState.terminalFailuresByKey.remove(trackedKey);
    if (retryState.isEmpty()) {
      retryStatesByEndpoint.remove(remoteEndpoint);
    }
    return terminalFailure;
  }
}
