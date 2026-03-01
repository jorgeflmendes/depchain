package pt.ulisboa.depchain.shared.network.links.fairloss;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.Objects;

import pt.ulisboa.depchain.shared.network.messages.InboundMessage;
import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.dpch.DpchSerialization;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class FairLossLink implements AutoCloseable {
  // The underlying DatagramSocket used for sending and receiving packets.
  private final DatagramSocket socket;

  // Lock object to synchronize send operations, since DatagramSocket is not thread-safe for concurrent sends.
  private final Object sendLock = new Object();

  // Lock object to serialize receive operations on the same socket instance.
  private final Object receiveLock = new Object();

  // The maximum allowed size of the serialized packet payload (excluding UDP/IP headers).
  private final int maxPacketSize;

  private FairLossLink(DatagramSocket socket, int maxPacketSize) throws SocketException {
    this.socket = Objects.requireNonNull(socket, "socket cannot be null");
    this.maxPacketSize = ValidationUtils.requirePositiveInt(maxPacketSize, "maxPacketSize");
    this.socket.setSoTimeout(0);
  }

  // Create and bind a socket to the given local address and port.
  public static FairLossLink bind(InetAddress bindAddress, int port, int maxPacketSize) throws IOException {
    DatagramSocket socket = new DatagramSocket(new InetSocketAddress(bindAddress, port));
    return new FairLossLink(socket, maxPacketSize);
  }

  // Create a socket that is not bound to any local port (client mode).
  public static FairLossLink unbound(int maxPacketSize) throws IOException {
    DatagramSocket socket = new DatagramSocket();
    return new FairLossLink(socket, maxPacketSize);
  }

  // Send one packet to the target endpoint.
  public void send(Dpch packet, InetAddress targetIp, int targetPort) throws IOException {
    Objects.requireNonNull(packet, "packet cannot be null");
    Objects.requireNonNull(targetIp, "targetIp cannot be null");
    sendPacket(packet, targetIp, targetPort);
  }

  // Wait until a packet is received (blocking call).
  public InboundMessage receive() throws IOException {
    DatagramPacket packet = receivePacket();
    Dpch decoded = decodePacket(packet);
    return new InboundMessage(decoded, packet.getAddress(), packet.getPort());
  }

  // Helper method to serialize and send a Dpch to the given target address and port.
  private void sendPacket(Dpch value, InetAddress targetIp, int targetPort) throws IOException {
    byte[] payload = DpchSerialization.toBytes(value);

    if (payload.length > maxPacketSize) {
      throw new IOException("Serialized payload exceeds maxPacketSize (%d > %d)".formatted(payload.length, maxPacketSize));
    }

    DatagramPacket packet = new DatagramPacket(payload, payload.length, targetIp, targetPort);
    synchronized (sendLock) {
      socket.send(packet);
    }
  }

  // Receive a packet from the socket.
  private DatagramPacket receivePacket() throws IOException {
    DatagramPacket packet = new DatagramPacket(new byte[maxPacketSize], maxPacketSize);
    synchronized (receiveLock) {
      socket.receive(packet);
    }
    return packet;
  }

  // Decode a DatagramPacket into a Dpch.
  private Dpch decodePacket(DatagramPacket packet) throws IOException {
    return DpchSerialization.fromBytes(packet.getData(), packet.getOffset(), packet.getLength());
  }

  @Override
  public void close() {
    socket.close();
  }
}
