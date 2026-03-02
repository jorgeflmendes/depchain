package pt.ulisboa.depchain.shared.network.model;

import java.net.InetSocketAddress;
import java.util.Objects;

// Stream identity by remote endpoint + connection id.
public record EndpointConnectionKey(InetSocketAddress endpoint, int connectionId) {
  public EndpointConnectionKey {
    Objects.requireNonNull(endpoint, "endpoint cannot be null");
  }
}
