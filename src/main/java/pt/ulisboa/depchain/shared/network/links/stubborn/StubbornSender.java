package pt.ulisboa.depchain.shared.network.links.stubborn;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import pt.ulisboa.depchain.shared.network.links.LinkFailureException;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedKey;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedMessage;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class StubbornSender {
  private record RetryCandidate(InetSocketAddress remoteEndpoint, TrackedMessage trackedMessage) {
  }

  private record TerminalFailureCandidate(InetSocketAddress remoteEndpoint, TrackedKey trackedKey) {
  }

  private final StubbornContext context;

  StubbornSender(StubbornContext context) {
    this.context = ValidationUtils.requireNonNull(context, "context");
  }

  // Sends a message one time only. Used for control messages (e.g., ACKs) that are not retried.
  void send(byte[] payload, InetSocketAddress remoteEndpoint) throws IOException {
    context.ensureOpen();
    Objects.requireNonNull(payload, "payload cannot be null");
    Objects.requireNonNull(remoteEndpoint, "remoteEndpoint cannot be null");
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
    Objects.requireNonNull(key, "key cannot be null");
    Objects.requireNonNull(payload, "payload cannot be null");
    Objects.requireNonNull(remoteEndpoint, "remoteEndpoint cannot be null");
    Objects.requireNonNull(onTerminalState, "onTerminalState cannot be null");

    TrackedMessage tracked = new TrackedMessage(key, payload, onTerminalState);

    synchronized (context.retryLock) {
      context.ensureOpen();
      tracked.scheduleRetryAfterNanos(context.retryDelayNanos(0));
      context.getOrCreateRetryState(remoteEndpoint).trackedMessagesByKey.put(key, tracked);
    }

    try {
      sendBestEffort(payload, remoteEndpoint, key);
    } catch (RuntimeException exception) {
      notifyTrackedSendFailure(remoteEndpoint, key, exception);
      throw exception;
    }
    return tracked.key();
  }

  void runRetrySweep() {
    List<RetryCandidate> dueRetries = new ArrayList<>();
    List<TerminalFailureCandidate> dueTerminalFailures = new ArrayList<>();
    List<TrackedMessage> terminalTrackedMessages = new ArrayList<>();
    long nowNanos = System.nanoTime();

    synchronized (context.retryLock) {
      if (!context.running.get()) {
        return;
      }

      for (var endpointEntry : context.retryStatesByEndpoint.entrySet()) {
        InetSocketAddress remoteEndpoint = endpointEntry.getKey();
        EndpointRetryState retryState = endpointEntry.getValue();

        for (var trackedEntry : retryState.trackedMessagesByKey.entrySet()) {
          TrackedKey trackedKey = trackedEntry.getKey();
          TrackedMessage trackedMessage = trackedEntry.getValue();
          if (trackedMessage.nextRetryAtNanos() > nowNanos) {
            continue;
          }

          if (context.reachedRetryLimit(trackedMessage)) {
            dueTerminalFailures.add(new TerminalFailureCandidate(remoteEndpoint, trackedKey));
            continue;
          }

          trackedMessage.advanceRetryAttempt();
          trackedMessage.scheduleRetryAfterNanos(context.retryDelayNanos(trackedMessage.retryAttempt()));
          dueRetries.add(new RetryCandidate(remoteEndpoint, trackedMessage));
        }
      }

      for (TerminalFailureCandidate dueTerminalFailure : dueTerminalFailures) {
        TrackedMessage terminalTracked = context.removeTrackedMessage(dueTerminalFailure.remoteEndpoint(), dueTerminalFailure.trackedKey());
        if (terminalTracked == null) {
          continue;
        }

        terminalTrackedMessages.add(terminalTracked);
        context.recordTerminalFailure(dueTerminalFailure.remoteEndpoint(), dueTerminalFailure
            .trackedKey(), new LinkFailureException("Tracked message failed after max retries: " + dueTerminalFailure.remoteEndpoint() + " / " + dueTerminalFailure.trackedKey()));
      }

      context.scheduleNextRetrySweep(this);
    }

    for (TrackedMessage terminalTrackedMessage : terminalTrackedMessages) {
      terminalTrackedMessage.notifyTerminalState();
    }

    for (RetryCandidate retryCandidate : dueRetries) {
      try {
        sendBestEffort(retryCandidate.trackedMessage().payloadView(), retryCandidate.remoteEndpoint(), retryCandidate.trackedMessage().key());
      } catch (RuntimeException exception) {
        notifyTrackedSendFailure(retryCandidate.remoteEndpoint(), retryCandidate.trackedMessage().key(), exception);
      }
    }
  }

  private void notifyTrackedSendFailure(InetSocketAddress remoteEndpoint, TrackedKey trackedKey, RuntimeException exception) {
    TrackedMessage removedTracked;
    synchronized (context.retryLock) {
      removedTracked = context.removeTrackedMessage(remoteEndpoint, trackedKey);
      context.recordTerminalFailure(remoteEndpoint, trackedKey, new LinkFailureException("Tracked message failed to send: " + remoteEndpoint + " / " + trackedKey, exception));
    }
    if (removedTracked != null) {
      removedTracked.notifyTerminalState();
    }
  }

  // Cancels the tracking of a message, preventing any future retries.
  void cancelTracked(TrackedKey key, InetSocketAddress remoteEndpoint) {
    Objects.requireNonNull(key, "key cannot be null");
    Objects.requireNonNull(remoteEndpoint, "remoteEndpoint cannot be null");
    if (!context.running.get()) {
      return;
    }

    synchronized (context.retryLock) {
      if (!context.running.get()) {
        return;
      }

      context.removeTrackedMessage(remoteEndpoint, key);
    }
  }

  LinkFailureException pollTerminalFailure(TrackedKey key, InetSocketAddress remoteEndpoint) {
    Objects.requireNonNull(key, "key cannot be null");
    Objects.requireNonNull(remoteEndpoint, "remoteEndpoint cannot be null");
    synchronized (context.retryLock) {
      return context.pollTerminalFailure(remoteEndpoint, key);
    }
  }
}
