package pt.ulisboa.depchain.shared.network.model;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.UUID;

// Stream identity by remote endpoint + connection id.
public record EndpointConnectionKey(InetSocketAddress endpoint, UUID connectionId) {
  public EndpointConnectionKey {
    Objects.requireNonNull(endpoint, "endpoint cannot be null");
    Objects.requireNonNull(connectionId, "connectionId cannot be null");
  }
}
