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

  // Sends a message (one time only) without tracking. Used for control messages (e.g., ACKs) that are
  // not retried.
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
  TrackedKey sendTracked(TrackedKey key, byte[] payload, InetSocketAddress remoteEndpoint) {
    context.ensureOpen();
    ValidationUtils.requireAllNonNull(named("key", key), named("payload", payload), named("remoteEndpoint", remoteEndpoint));

    long now = TimeUtil.nowMs();
    TrackedMessage tracked;

    synchronized (context.stateLock) {
      context.ensureOpen();
      TrackedTargetKey targetKey = new TrackedTargetKey(remoteEndpoint, key);
      if (context.retryRegistry.shouldSkipTrackedRegistration(targetKey, now)) { // A pending cancel was recorded for this key, so skip registration
        return key;
      }

      long nextRetryAtMs = TimeUtil.deadlineAfter(now, context.computeRetryDelayMs(0));
      tracked = new TrackedMessage(key, payload, 0, now, nextRetryAtMs);
      context.retryRegistry.putTracked(targetKey, tracked);
      context.stateLock.notifyAll();
    }

    sendBestEffort(tracked.payload(), remoteEndpoint, tracked.key());
    return tracked.key();
  }

  // Cancels the tracking of a message, preventing any future retries.
  void cancelTracked(TrackedKey key, InetSocketAddress remoteEndpoint) {
    context.ensureOpen();
    ValidationUtils.requireAllNonNull(named("key", key), named("remoteEndpoint", remoteEndpoint));

    long now = TimeUtil.nowMs();
    synchronized (context.stateLock) {
      context.ensureOpen();

      TrackedTargetKey targetKey = new TrackedTargetKey(remoteEndpoint, key);
      if (context.retryRegistry.removeTracked(targetKey) == null) { // No tracked was removed, so record a pending cancel to prevent future registration (race
                                                                    // condition).
        context.retryRegistry.recordPendingCancel(targetKey, now);
      }

      context.stateLock.notifyAll();
    }
  }

  LinkFailureException trackedFailureOrNull(TrackedKey key, InetSocketAddress remoteEndpoint) {
    ValidationUtils.requireAllNonNull(named("key", key), named("remoteEndpoint", remoteEndpoint));
    synchronized (context.stateLock) {
      return context.retryRegistry.trackedFailureOrNull(new TrackedTargetKey(remoteEndpoint, key));
    }
  }
}
