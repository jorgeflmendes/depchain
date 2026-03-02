package pt.ulisboa.depchain.shared.network.links.fairloss;

import java.net.InetAddress;
import java.util.Objects;

import pt.ulisboa.depchain.shared.utils.ValidationUtils;

// Raw inbound UDP datagram with sender endpoint metadata.
public record InboundDatagram(byte[] payload, InetAddress senderIp, int senderPort) {
  public InboundDatagram {
    Objects.requireNonNull(payload, "payload cannot be null");
    Objects.requireNonNull(senderIp, "senderIp cannot be null");
    ValidationUtils.requireValidPort(senderPort, "senderPort");
  }
}
