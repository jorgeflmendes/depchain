package pt.ulisboa.depchain.shared.network.model;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.net.InetSocketAddress;

import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

// Common inbound packet shape shared by link layers.
public record InboundPacket(InetSocketAddress sender, Dpch packet) {
  public InboundPacket {
    ValidationUtils.requireAllNonNull(named("sender", sender), named("packet", packet));
    ValidationUtils.requireValidPort(sender.getPort(), "sender.port");
  }
}
