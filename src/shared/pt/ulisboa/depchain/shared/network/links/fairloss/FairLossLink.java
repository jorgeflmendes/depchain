package pt.ulisboa.depchain.shared.network.links.fairloss;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Arrays;

import pt.ulisboa.depchain.shared.network.links.BlockingLink;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class FairLossLink implements BlockingLink<InboundBytes> {
  public static final int DEFAULT_MAX_PACKET_SIZE = 8_192;
  private final int maxPacketSize;

  private final DatagramSocket socket;

  // Reused receive buffer/packet to avoid per-receive allocations.
  private final byte[] receiveBuffer;
  private final DatagramPacket receivePacket;

  // Protects only the reused receiveBuffer/receivePacket.
  private final Object receiveLock = new Object();

  private FairLossLink(DatagramSocket socket, int maxPacketSize) throws SocketException {
    this.socket = ValidationUtils.requireNonNull(socket, "socket");
    this.maxPacketSize = ValidationUtils.requirePositiveInt(maxPacketSize, "maxPacketSize");
    this.receiveBuffer = new byte[this.maxPacketSize];
    this.receivePacket = new DatagramPacket(this.receiveBuffer, this.receiveBuffer.length);
    this.socket.setSoTimeout(0);
  }

  public static FairLossLink bind(InetSocketAddress bindEndpoint, int maxPacketSize) throws IOException {
    ValidationUtils.requireNonNull(bindEndpoint, "bindEndpoint");
    DatagramSocket socket = new DatagramSocket(bindEndpoint);
    return new FairLossLink(socket, maxPacketSize);
  }

  public static FairLossLink unbound(int maxPacketSize) throws IOException {
    DatagramSocket socket = new DatagramSocket();
    return new FairLossLink(socket, maxPacketSize);
  }

  public void send(byte[] payload, InetSocketAddress remoteEndpoint) throws IOException {
    ValidationUtils.requireAllNonNull(named("payload", payload), named("remoteEndpoint", remoteEndpoint));
    ValidationUtils.requireNonNull(remoteEndpoint.getAddress(), "remoteEndpoint.address");
    ValidationUtils.requireValidPort(remoteEndpoint.getPort(), "remoteEndpoint.port");
    if (payload.length > maxPacketSize) {
      throw new IOException("Serialized payload exceeds maxPacketSize (%d > %d)".formatted(payload.length, maxPacketSize));
    }

    DatagramPacket datagram = new DatagramPacket(payload, payload.length, remoteEndpoint.getAddress(), remoteEndpoint.getPort());
    socket.send(datagram);
  }

  @Override
  public InboundBytes receive() throws IOException {
    return receiveInternal(0);
  }

  @Override
  public InboundBytes receive(long timeoutMs) throws IOException {
    return receiveInternal(ValidationUtils.requireNonNegativeLong(timeoutMs, "timeoutMs"));
  }

  private InboundBytes receiveInternal(long timeoutMs) throws IOException {
    synchronized (receiveLock) {
      receivePacket.setData(receiveBuffer, 0, receiveBuffer.length);
      receivePacket.setLength(receiveBuffer.length);
      int previousTimeoutMs = socket.getSoTimeout();
      socket.setSoTimeout((int) Math.min(Integer.MAX_VALUE, timeoutMs));

      try {
        socket.receive(receivePacket);
      } catch (SocketTimeoutException ignored) {
        return null;
      } finally {
        socket.setSoTimeout(previousTimeoutMs);
      }

      byte[] payload = Arrays.copyOf(receiveBuffer, receivePacket.getLength());
      InetSocketAddress sender = new InetSocketAddress(receivePacket.getAddress(), receivePacket.getPort());
      return new InboundBytes(sender, payload);
    }
  }

  @Override
  public void close() {
    socket.close();
  }
}
