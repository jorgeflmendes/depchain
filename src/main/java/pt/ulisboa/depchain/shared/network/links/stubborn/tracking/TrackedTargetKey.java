package pt.ulisboa.depchain.shared.network.links.stubborn.tracking;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.net.InetSocketAddress;

import pt.ulisboa.depchain.shared.utils.ValidationUtils;

// Represents a key for tracking messages sent to an endpoint.
public record TrackedTargetKey(InetSocketAddress endpoint, TrackedKey key) {
  public TrackedTargetKey {
    ValidationUtils.requireAllNonNull(named("endpoint", endpoint), named("key", key));
  }
}
