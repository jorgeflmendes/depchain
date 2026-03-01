package pt.ulisboa.depchain.shared.network.links.stubborn;

import java.net.InetSocketAddress;
import java.util.Objects;

import pt.ulisboa.depchain.shared.network.dpch.Dpch;

// Event types handled by the stubborn loop thread.
sealed interface Event permits SendTrackedEvent, CancelTrackedEvent, ForceResendEvent, ShutdownEvent {}

// Event for registering a tracked send.
record SendTrackedEvent(InetSocketAddress endpoint, TrackedMessage.Key key, Dpch packet) implements Event {
  // Validate send event arguments.
  SendTrackedEvent {
    Objects.requireNonNull(endpoint, "endpoint cannot be null");
    Objects.requireNonNull(key, "key cannot be null");
    Objects.requireNonNull(packet, "packet cannot be null");
  }
}

// Event for canceling retries of one tracked key.
record CancelTrackedEvent(InetSocketAddress endpoint, TrackedMessage.Key key) implements Event {
  // Validate cancel event arguments.
  CancelTrackedEvent {
    Objects.requireNonNull(endpoint, "endpoint cannot be null");
    Objects.requireNonNull(key, "key cannot be null");
  }
}

// Event for forcing an immediate retry.
record ForceResendEvent(InetSocketAddress endpoint, TrackedMessage.Key key) implements Event {
  // Validate force-resend event arguments.
  ForceResendEvent {
    Objects.requireNonNull(endpoint, "endpoint cannot be null");
    Objects.requireNonNull(key, "key cannot be null");
  }
}

// Event used to terminate the loop.
record ShutdownEvent() implements Event {}
