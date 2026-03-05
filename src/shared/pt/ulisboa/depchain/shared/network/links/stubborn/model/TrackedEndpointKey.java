package pt.ulisboa.depchain.shared.network.links.stubborn.model;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.net.InetSocketAddress;

import pt.ulisboa.depchain.shared.utils.ValidationUtils;

// Represents a key for tracking messages sent to an endpoint.
public record TrackedEndpointKey(InetSocketAddress endpoint, TrackedMessage.Key key) {
  public TrackedEndpointKey {
    ValidationUtils.requireAllNonNull(named("endpoint", endpoint), named("key", key));
  }
}
