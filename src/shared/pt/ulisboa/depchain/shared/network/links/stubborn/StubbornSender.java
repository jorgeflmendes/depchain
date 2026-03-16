package pt.ulisboa.depchain.shared.network.links.stubborn;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.io.IOException;
import java.net.InetSocketAddress;

import pt.ulisboa.depchain.shared.network.links.LinkFailureException;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedKey;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedMessage;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedTargetKey;
import pt.ulisboa.depchain.shared.utils.TimeUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

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
  void sendBestEffort(byte[] payload, InetSocketAddress endpoint, TrackedKey key) {
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

    synchronized (context.stateLock) {
      context.ensureOpen();
      long nextRetryAtMs = TimeUtil.deadlineAfter(now, context.computeRetryDelayMs(0));
      tracked = new TrackedMessage(key, payload, 0, now, nextRetryAtMs, onTerminalState);
      context.retryRegistry.putTrackedMessage(targetKey, tracked);
      context.stateLock.notifyAll();
    }

    try {
      sendBestEffort(tracked.payloadView(), remoteEndpoint, tracked.key());
    } catch (RuntimeException exception) {
      TrackedMessage removedTracked;
      synchronized (context.stateLock) {
        removedTracked = context.retryRegistry.removeTrackedMessage(targetKey);
        context.retryRegistry.recordTerminalFailure(targetKey, new LinkFailureException("Tracked message failed to send: " + targetKey));
        context.stateLock.notifyAll();
      }
      if (removedTracked != null) {
        removedTracked.notifyTerminalState();
      }
      throw exception;
    }
    return tracked.key();
  }

  // Cancels the tracking of a message, preventing any future retries.
  void cancelTracked(TrackedKey key, InetSocketAddress remoteEndpoint) {
    ValidationUtils.requireAllNonNull(named("key", key), named("remoteEndpoint", remoteEndpoint));
    if (!context.running.get()) {
      return;
    }

    synchronized (context.stateLock) {
      if (!context.running.get()) {
        return;
      }

      TrackedTargetKey targetKey = new TrackedTargetKey(remoteEndpoint, key);
      context.retryRegistry.removeTrackedMessage(targetKey);
      context.stateLock.notifyAll();
    }
  }

  LinkFailureException pollTerminalFailure(TrackedKey key, InetSocketAddress remoteEndpoint) {
    ValidationUtils.requireAllNonNull(named("key", key), named("remoteEndpoint", remoteEndpoint));
    synchronized (context.stateLock) {
      return context.retryRegistry.pollTerminalFailure(new TrackedTargetKey(remoteEndpoint, key));
    }
  }
}
