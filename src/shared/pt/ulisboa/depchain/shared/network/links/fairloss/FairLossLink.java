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
  public static final int MAX_PACKET_SIZE = 8_192;

  private final DatagramSocket socket;
  private final byte[] receiveBuffer;
  private final DatagramPacket receivePacket;
  private int configuredSoTimeoutMs;

  private FairLossLink(DatagramSocket socket) throws SocketException {
    this.socket = ValidationUtils.requireNonNull(socket, "socket");
    this.receiveBuffer = new byte[MAX_PACKET_SIZE];
    this.receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
    this.configuredSoTimeoutMs = 0;
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
    ValidationUtils.requireAllNonNull(named("payload", payload), named("remoteEndpoint", remoteEndpoint));
    ValidationUtils.requireNonNull(remoteEndpoint.getAddress(), "remoteEndpoint.address");
    ValidationUtils.requireValidPort(remoteEndpoint.getPort(), "remoteEndpoint.port");
    if (payload.length > MAX_PACKET_SIZE) {
      throw new IllegalArgumentException("Serialized payload exceeds MAX_PACKET_SIZE (%d > %d)".formatted(payload.length, MAX_PACKET_SIZE));
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

  private synchronized InboundBytes receiveInternal(long timeoutMs) throws IOException {
    configureReceiveTimeout((int) Math.min(Integer.MAX_VALUE, timeoutMs));
    receivePacket.setData(receiveBuffer, 0, receiveBuffer.length);

    try {
      socket.receive(receivePacket);
    } catch (SocketTimeoutException ignored) {
      return null;
    }

    byte[] payload = Arrays.copyOf(receiveBuffer, receivePacket.getLength());
    InetSocketAddress sender = new InetSocketAddress(receivePacket.getAddress(), receivePacket.getPort());
    return new InboundBytes(sender, payload);
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

