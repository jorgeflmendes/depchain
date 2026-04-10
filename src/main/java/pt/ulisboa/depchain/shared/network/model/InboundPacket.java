package pt.ulisboa.depchain.shared.network.model;

import java.net.InetSocketAddress;
import java.util.Objects;

import com.google.protobuf.ByteString;

import pt.ulisboa.depchain.proto.DpchPacket;

// Common inbound packet shape shared by link layers.
public record InboundPacket(InetSocketAddress sender, DpchPacket packet, ByteString payload, Long authenticatedSenderId) {
  public InboundPacket {
    Objects.requireNonNull(sender, "sender");
    Objects.requireNonNull(packet, "packet");
    int port = sender.getPort();
    if (port < 0 || port > 65535) {
      throw new IllegalArgumentException("sender.port must be a valid port");
    }
    payload = payload == null ? packet.getPayload() : payload;
    Objects.requireNonNull(payload, "payload");
    if (authenticatedSenderId != null && authenticatedSenderId < 0L) {
      throw new IllegalArgumentException("authenticatedSenderId must be non-negative");
    }
  }

  public InboundPacket(InetSocketAddress sender, DpchPacket packet, ByteString payload) {
    this(sender, packet, payload, null);
  }

  public InboundPacket(InetSocketAddress sender, DpchPacket packet) {
    this(sender, packet, null, null);
  }

  public InboundPacket(InetSocketAddress sender, DpchPacket packet, Long authenticatedSenderId) {
    this(sender, packet, null, authenticatedSenderId);
  }
}
