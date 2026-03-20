package pt.ulisboa.depchain.shared.network.model;

import java.net.InetSocketAddress;

import pt.ulisboa.depchain.shared.utils.ValidationUtils;

// Connection identity by remote endpoint + connection id.
public record ConnectionKey(InetSocketAddress endpoint, long connectionId) {
  public ConnectionKey {
    ValidationUtils.requireNonNull(endpoint, "endpoint");
  }
}
