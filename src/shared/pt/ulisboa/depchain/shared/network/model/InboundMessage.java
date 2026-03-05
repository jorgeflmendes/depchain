package pt.ulisboa.depchain.shared.network.model;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;
import static pt.ulisboa.depchain.shared.utils.ValidationUtils.requireAllNonNull;

import java.net.InetAddress;

import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

// Common inbound packet shape shared by link layers.
public record InboundMessage(Dpch packet, InetAddress senderIp, int senderPort) {
  public InboundMessage {
    requireAllNonNull(named("packet", packet), named("senderIp", senderIp));
    ValidationUtils.requireValidPort(senderPort, "senderPort");
  }
}

