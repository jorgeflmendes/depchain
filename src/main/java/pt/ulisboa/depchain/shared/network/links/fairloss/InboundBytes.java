package pt.ulisboa.depchain.shared.network.links.fairloss;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.net.InetSocketAddress;

import pt.ulisboa.depchain.shared.utils.ValidationUtils;

// Raw inbound UDP datagram (bytes) with sender endpoint metadata.
public record InboundBytes(InetSocketAddress sender, byte[] payload) {
  public InboundBytes {
    ValidationUtils.requireAllNonNull(named("sender", sender), named("payload", payload));
    ValidationUtils.requireValidPort(sender.getPort(), "sender.port");
  }
}
