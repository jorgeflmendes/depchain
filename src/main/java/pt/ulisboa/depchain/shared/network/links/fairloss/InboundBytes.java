package pt.ulisboa.depchain.shared.network.links.fairloss;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Objects;

// Raw inbound UDP datagram with sender metadata and a zero-copy view over the payload bytes.
public final class InboundBytes {
  private final InetSocketAddress sender;
  private final byte[] payloadBuffer;
  private final int payloadLength;

  public InboundBytes(InetSocketAddress sender, byte[] payload) {
    this(sender, payload, payload.length);
  }

  InboundBytes(InetSocketAddress sender, byte[] payloadBuffer, int payloadLength) {
    this.sender = Objects.requireNonNull(sender, "sender cannot be null");
    this.payloadBuffer = Objects.requireNonNull(payloadBuffer, "payloadBuffer cannot be null");
    if (payloadLength < 0 || payloadLength > payloadBuffer.length) {
      throw new IllegalArgumentException("payloadLength must be in range [0, payloadBuffer.length]");
    }
    this.payloadLength = payloadLength;
  }

  public InetSocketAddress sender() {
    return sender;
  }

  public byte[] payload() {
    return Arrays.copyOf(payloadBuffer, payloadLength);
  }

  public byte[] payloadView() {
    return payloadBuffer;
  }

  public int payloadLength() {
    return payloadLength;
  }
}
