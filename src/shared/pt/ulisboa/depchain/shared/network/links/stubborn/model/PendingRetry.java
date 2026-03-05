package pt.ulisboa.depchain.shared.network.links.stubborn.model;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;
import static pt.ulisboa.depchain.shared.utils.ValidationUtils.requireAllNonNull;

import java.net.InetSocketAddress;

// Represents a message that is pending to be retried for sending.
public record PendingRetry(byte[] payload, TrackedMessage.Key key, InetSocketAddress endpoint) {
  public PendingRetry {
    requireAllNonNull(named("payload", payload), named("key", key), named("endpoint", endpoint));
  }
}
