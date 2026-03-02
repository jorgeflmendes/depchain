package pt.ulisboa.depchain.shared.network.model;

import java.net.InetAddress;
import java.util.Objects;

import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

// Common inbound packet shape shared by link layers.
public record InboundMessage(Dpch packet, InetAddress senderIp, int senderPort) {
  public InboundMessage {
    Objects.requireNonNull(packet, "packet cannot be null");
    Objects.requireNonNull(senderIp, "senderIp cannot be null");
    ValidationUtils.requireValidPort(senderPort, "senderPort");
  }
}
