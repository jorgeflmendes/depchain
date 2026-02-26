package pt.ulisboa.depchain.shared.links.fairloss.transport;

import java.net.InetAddress;
import java.util.Objects;

import pt.ulisboa.depchain.shared.links.fairloss.message.Dpch;

// To save the sender IP and port of an incoming request, along with the parsed DPCH packet.
public record InboundRequest(Dpch packet, InetAddress senderIp, int senderPort) {
  public InboundRequest {
    Objects.requireNonNull(packet, "packet cannot be null");
    Objects.requireNonNull(senderIp, "senderIp cannot be null");

    if (senderPort < 1 || senderPort > 65535) {
      throw new IllegalArgumentException("senderPort must be in range [1, 65535]");
    }
  }
}
