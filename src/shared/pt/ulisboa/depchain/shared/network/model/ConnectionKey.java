package pt.ulisboa.depchain.shared.network.model;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import pt.ulisboa.depchain.shared.utils.ValidationUtils;

// Connection identity by remote endpoint + connection id.
public record ConnectionKey(InetSocketAddress endpoint, long connectionId) {
  public ConnectionKey {
    ValidationUtils.requireNonNull(endpoint, "endpoint");
  }

  public static ConnectionKey from(InetAddress remoteIp, int remotePort, long connectionId) {
    return new ConnectionKey(new InetSocketAddress(remoteIp, remotePort), connectionId);
  }

  public static ConnectionKey from(InetSocketAddress remoteEndpoint, long connectionId) {
    return new ConnectionKey(remoteEndpoint, connectionId);
  }
}
