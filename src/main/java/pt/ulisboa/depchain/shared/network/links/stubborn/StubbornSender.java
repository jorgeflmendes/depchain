package pt.ulisboa.depchain.shared.network.links.stubborn;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import pt.ulisboa.depchain.shared.network.links.LinkFailureException;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedKey;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedMessage;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedTargetKey;
import pt.ulisboa.depchain.shared.utils.TimeUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

import io.netty.util.Timeout;

final class StubbornSender {
  private final StubbornContext context;

  StubbornSender(StubbornContext context) {
    this.context = ValidationUtils.requireNonNull(context, "context");
  }

  // Sends a message one time only. Used for control messages (e.g., ACKs) that are not retried.
  void send(byte[] payload, InetSocketAddress remoteEndpoint) throws IOException {
    context.ensureOpen();
    ValidationUtils.requireAllNonNull(named("payload", payload), named("remoteEndpoint", remoteEndpoint));
    context.fairLossLink.send(payload, remoteEndpoint);
  }

  // Sends a message (one time only) without tracking, ignoring any exceptions.
  private void sendBestEffort(byte[] payload, InetSocketAddress endpoint, TrackedKey key) {
    try {
      context.fairLossLink.send(payload, endpoint);
    } catch (IOException exception) {
      throw new IllegalStateException("Failed to send tracked packet " + key, exception);
    }
  }

  // Sends a message and tracks it for retries until an ACK is received or the sender is closed.
  TrackedKey sendTrackedWithTerminalNotification(TrackedKey key, byte[] payload, InetSocketAddress remoteEndpoint, Runnable onTerminalState) {
    context.ensureOpen();
    ValidationUtils.requireAllNonNull(named("key", key), named("payload", payload), named("remoteEndpoint", remoteEndpoint), named("onTerminalState", onTerminalState));

    long now = TimeUtil.nowMs();
    TrackedMessage tracked;
    TrackedTargetKey targetKey = new TrackedTargetKey(remoteEndpoint, key);

    synchronized (context.retryLock) {
      context.ensureOpen();
      long nextRetryAtMs = TimeUtil.deadlineAfter(now, context.retryDelayMs(0));
      tracked = new TrackedMessage(key, payload, 0, now, nextRetryAtMs, onTerminalState);
      context.trackedMessagesByTarget.put(targetKey, tracked);
      scheduleRetry(targetKey, tracked);
    }

    try {
      sendBestEffort(tracked.payloadView(), remoteEndpoint, tracked.key());
    } catch (RuntimeException exception) {
      notifyTrackedSendFailure(targetKey, exception);
      throw exception;
    }
    return tracked.key();
  }

    private void scheduleRetry(TrackedTargetKey targetKey, TrackedMessage tracked) {
    long delayMs = Math.max(1L, TimeUtil.remainingMsUntil(tracked.nextRetryAtMs(), TimeUtil.nowMs()));
    Timeout timeout = context.retryTimer.newTimeout(ignored -> retryTracked(targetKey), delayMs, TimeUnit.MILLISECONDS);
    context.replaceRetryTimeout(targetKey, timeout);
  }

  private void retryTracked(TrackedTargetKey targetKey) {
    TrackedMessage tracked;
    TrackedMessage terminalTracked = null;

    synchronized (context.retryLock) {
      if (!context.running.get()) {
        return;
      }

      tracked = context.trackedMessagesByTarget.get(targetKey);
      if (tracked == null) {
        return;
      }

      if (context.reachedRetryLimit(tracked)) {
        terminalTracked = context.removeTrackedMessage(targetKey);
        context.terminalFailuresByTarget.put(targetKey, new LinkFailureException("Tracked message failed after max retries: " + targetKey));
      } else {
        long now = TimeUtil.nowMs();
        tracked.markRetried(TimeUtil.deadlineAfter(now, context.retryDelayMs(tracked.retryAttempt() + 1)));
        scheduleRetry(targetKey, tracked);
      }
    }

    if (terminalTracked != null) {
      terminalTracked.notifyTerminalState();
      return;
    }

    try {
      sendBestEffort(tracked.payloadView(), targetKey.endpoint(), tracked.key());
    } catch (RuntimeException exception) {
      notifyTrackedSendFailure(targetKey, exception);
    }
  }

  private void notifyTrackedSendFailure(TrackedTargetKey targetKey, RuntimeException exception) {
    TrackedMessage removedTracked;
    synchronized (context.retryLock) {
      removedTracked = context.removeTrackedMessage(targetKey);
      context.terminalFailuresByTarget.put(targetKey, new LinkFailureException("Tracked message failed to send: " + targetKey, exception));
    }
    if (removedTracked != null) {
      removedTracked.notifyTerminalState();
    }
  }

  // Cancels the tracking of a message, preventing any future retries.
  void cancelTracked(TrackedKey key, InetSocketAddress remoteEndpoint) {
    ValidationUtils.requireAllNonNull(named("key", key), named("remoteEndpoint", remoteEndpoint));
    if (!context.running.get()) {
      return;
    }

    synchronized (context.retryLock) {
      if (!context.running.get()) {
        return;
      }

      TrackedTargetKey targetKey = new TrackedTargetKey(remoteEndpoint, key);
      context.removeTrackedMessage(targetKey);
    }
  }

  LinkFailureException pollTerminalFailure(TrackedKey key, InetSocketAddress remoteEndpoint) {
    ValidationUtils.requireAllNonNull(named("key", key), named("remoteEndpoint", remoteEndpoint));
    synchronized (context.retryLock) {
      return context.pollTerminalFailure(new TrackedTargetKey(remoteEndpoint, key));
    }
  }
}
