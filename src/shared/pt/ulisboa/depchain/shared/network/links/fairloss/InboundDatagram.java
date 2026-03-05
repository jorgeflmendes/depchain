package pt.ulisboa.depchain.shared.network.links.fairloss;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.net.InetAddress;

import pt.ulisboa.depchain.shared.utils.ValidationUtils;

// Raw inbound UDP datagram with sender endpoint metadata.
public record InboundDatagram(byte[] payload, InetAddress senderIp, int senderPort) {
  public InboundDatagram {
    ValidationUtils.requireAllNonNull(named("payload", payload), named("senderIp", senderIp));
    ValidationUtils.requireValidPort(senderPort, "senderPort");
  }
}

