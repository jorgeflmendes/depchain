package pt.ulisboa.depchain.shared.links.fairloss.transport;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.Objects;

import pt.ulisboa.depchain.shared.links.fairloss.codec.DpchCodec;
import pt.ulisboa.depchain.shared.links.fairloss.message.Dpch;

public final class FairLossLink implements AutoCloseable {
  // The underlying DatagramSocket used for sending and receiving packets.
  private final DatagramSocket socket;

  // Lock object to synchronize send operations, since DatagramSocket is not thread-safe for concurrent sends.
  private final Object sendLock = new Object();

  // The maximum allowed size of the serialized packet payload (excluding UDP/IP headers).
  private final int maxPacketSize;

  private FairLossLink(DatagramSocket socket, int maxPacketSize) throws SocketException {
    this.socket = Objects.requireNonNull(socket, "socket cannot be null");

    if (maxPacketSize <= 0) {
      throw new IllegalArgumentException("maxPacketSize must be > 0");
    }

    this.maxPacketSize = maxPacketSize;
    this.socket.setSoTimeout(0);
  }

  // Create and bind a socket to the given local address and port.
  public static FairLossLink bind(InetAddress bindAddress, int port, int maxPacketSize) throws IOException {
    DatagramSocket socket = new DatagramSocket(new InetSocketAddress(bindAddress, port));
    return new FairLossLink(socket, maxPacketSize);
  }

  // Create a socket that is not bound to any local port.
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
  public InboundRequest receive() throws IOException {
    while (true) {
      DatagramPacket packet = receivePacket();
      Dpch decoded = decodePacketOrNull(packet);
      if (decoded != null) {
        return new InboundRequest(decoded, packet.getAddress(), packet.getPort());
      }
    }
  }

  // Helper method to serialize and send a Dpch to the given target address and port.
  private void sendPacket(Dpch value, InetAddress targetIp, int targetPort) throws IOException {
    byte[] payload = DpchCodec.toBytes(value);

    if (payload.length > maxPacketSize) {
      throw new IOException(
          "Serialized payload exceeds maxPacketSize (%d > %d)".formatted(payload.length, maxPacketSize));
    }

    DatagramPacket packet = new DatagramPacket(payload, payload.length, targetIp, targetPort);
    synchronized (sendLock) {
      socket.send(packet);
    }
  }

  // Helper method to receive a packet from the socket.
  private DatagramPacket receivePacket() throws IOException {
    DatagramPacket packet = new DatagramPacket(new byte[maxPacketSize], maxPacketSize);
    socket.receive(packet);
    return packet;
  }

  // Helper method to decode a DatagramPacket into a Dpch, or return null if decoding fails.
  private Dpch decodePacketOrNull(DatagramPacket packet) {
    try {
      return DpchCodec.fromBytes(packet.getData(), packet.getOffset(), packet.getLength());
    } catch (IOException ignored) {
      return null;
    }
  }

  @Override
  public void close() {
    socket.close();
  }
}
