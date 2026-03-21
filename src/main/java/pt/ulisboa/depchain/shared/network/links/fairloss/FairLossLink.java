package pt.ulisboa.depchain.shared.network.links.fairloss;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Objects;

import org.eclipse.jdt.annotation.Nullable;

import pt.ulisboa.depchain.shared.network.links.BlockingLink;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class FairLossLink implements BlockingLink<InboundBytes> {
  public static final int MAX_PACKET_SIZE = 8_192;
  private static final int RECEIVE_BUFFER_RING_SIZE = 32;

  private final DatagramSocket socket;
  private final byte[][] receiveBufferRing;
  private final DatagramPacket receivePacket;
  private final ThreadLocal<DatagramPacket> sendPacketByThread;
  private int configuredSoTimeoutMs;
  private int nextReceiveBufferIndex;

  private FairLossLink(DatagramSocket socket) throws SocketException {
    this.socket = ValidationUtils.requireNonNull(socket, "socket");
    this.receiveBufferRing = new byte[RECEIVE_BUFFER_RING_SIZE][MAX_PACKET_SIZE];
    this.receivePacket = new DatagramPacket(receiveBufferRing[0], MAX_PACKET_SIZE);
    this.sendPacketByThread = ThreadLocal.withInitial(() -> new DatagramPacket(new byte[0], 0));
    this.configuredSoTimeoutMs = 0;
    this.nextReceiveBufferIndex = 0;
    this.socket.setSoTimeout(0);
  }

  public static FairLossLink bind(InetSocketAddress bindEndpoint) throws IOException {
    ValidationUtils.requireNonNull(bindEndpoint, "bindEndpoint");

    DatagramSocket socket = new DatagramSocket(bindEndpoint);
    return new FairLossLink(socket);
  }

  public static FairLossLink unbound() throws IOException {
    DatagramSocket socket = new DatagramSocket();
    return new FairLossLink(socket);
  }

  public void send(byte[] payload, InetSocketAddress remoteEndpoint) throws IOException {
    Objects.requireNonNull(payload, "payload cannot be null");
    Objects.requireNonNull(remoteEndpoint, "remoteEndpoint cannot be null");
    if (payload.length > MAX_PACKET_SIZE) {
      throw new IllegalArgumentException("Serialized payload exceeds MAX_PACKET_SIZE (%d > %d)".formatted(payload.length, MAX_PACKET_SIZE));
    }

    DatagramPacket datagram = sendPacketByThread.get();
    datagram.setData(payload, 0, payload.length);
    datagram.setSocketAddress(remoteEndpoint);
    socket.send(datagram);
  }

  @Override
  public InboundBytes receive() throws IOException {
    return receiveInternal(0);
  }

  @Override
  public @Nullable InboundBytes receive(long timeoutMs) throws IOException {
    return receiveInternal(ValidationUtils.requireNonNegativeLong(timeoutMs, "timeoutMs"));
  }

  private @Nullable InboundBytes receiveInternal(long timeoutMs) throws IOException {
    configureReceiveTimeout((int) Math.min(Integer.MAX_VALUE, timeoutMs));
    byte[] receiveBuffer = nextReceiveBuffer();
    receivePacket.setData(receiveBuffer, 0, receiveBuffer.length);
    receivePacket.setLength(receiveBuffer.length);

    try {
      socket.receive(receivePacket);
    } catch (SocketTimeoutException ignored) {
      return null;
    }

    int payloadLength = receivePacket.getLength();
    InetSocketAddress sender = new InetSocketAddress(receivePacket.getAddress(), receivePacket.getPort());
    return new InboundBytes(sender, receiveBuffer, payloadLength);
  }

  private byte[] nextReceiveBuffer() {
    byte[] receiveBuffer = receiveBufferRing[nextReceiveBufferIndex];
    nextReceiveBufferIndex = (nextReceiveBufferIndex + 1) & (RECEIVE_BUFFER_RING_SIZE - 1);
    return receiveBuffer;
  }

  private void configureReceiveTimeout(int timeoutMs) throws SocketException {
    if (configuredSoTimeoutMs == timeoutMs) {
      return;
    }

    socket.setSoTimeout(timeoutMs);
    configuredSoTimeoutMs = timeoutMs;
  }

  @Override
  public void close() {
    socket.close();
  }
}
