package pt.ulisboa.depchain.shared.network.links.fairloss;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.Objects;

import pt.ulisboa.depchain.shared.network.model.InboundMessage;
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
  
  // Reused receive buffer and packet holder to avoid per-receive allocations.
  private final byte[] receiveBuffer;
  private final DatagramPacket receivePacket;

  private FairLossLink(DatagramSocket socket, int maxPacketSize) throws SocketException {
    this.socket = Objects.requireNonNull(socket, "socket cannot be null");
    this.maxPacketSize = ValidationUtils.requirePositiveInt(maxPacketSize, "maxPacketSize");
    this.receiveBuffer = new byte[this.maxPacketSize];
    this.receivePacket = new DatagramPacket(this.receiveBuffer, this.receiveBuffer.length);
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
  public void send(Dpch packet, InetAddress remoteIp, int remotePort) throws IOException {
    Objects.requireNonNull(packet, "packet cannot be null");
    Objects.requireNonNull(remoteIp, "remoteIp cannot be null");
    byte[] payload = DpchSerialization.toBytes(packet);
    if (payload.length > maxPacketSize) {
      throw new IOException("Serialized payload exceeds maxPacketSize (%d > %d)".formatted(payload.length, maxPacketSize));
    }

    DatagramPacket datagram = new DatagramPacket(payload, payload.length, remoteIp, remotePort);
    synchronized (sendLock) {
      socket.send(datagram);
    }
  }

  // Wait until a packet is received (blocking call).
  public InboundMessage receive() throws IOException {
    synchronized (receiveLock) {
      receivePacket.setData(receiveBuffer, 0, receiveBuffer.length);
      receivePacket.setLength(receiveBuffer.length);
      socket.receive(receivePacket);
      Dpch decoded = DpchSerialization.fromBytes(receivePacket.getData(), receivePacket.getOffset(), receivePacket.getLength());

      return new InboundMessage(decoded, receivePacket.getAddress(), receivePacket.getPort());
    }
  }

  @Override
  public void close() {
    socket.close();
  }
}
