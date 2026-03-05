package pt.ulisboa.depchain.shared.network.links.fairloss;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.Arrays;

import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class FairLossLink implements AutoCloseable {
  private final int maxPacketSize;
  private final DatagramSocket socket;
  
  // Reused receive buffer and packet holder to avoid per-receive allocations.
  private final byte[] receiveBuffer;
  private final DatagramPacket receivePacket;

  // Locks to synchronize send and receive operations since DatagramSocket is not thread-safe.
  private final Object sendLock = new Object();
  private final Object receiveLock = new Object();

  private FairLossLink(DatagramSocket socket, int maxPacketSize) throws SocketException {
    this.socket = ValidationUtils.requireNonNull(socket, "socket");
    this.maxPacketSize = ValidationUtils.requirePositiveInt(maxPacketSize, "maxPacketSize");
    this.receiveBuffer = new byte[this.maxPacketSize];
    this.receivePacket = new DatagramPacket(this.receiveBuffer, this.receiveBuffer.length);
    this.socket.setSoTimeout(0);
  }

  public static FairLossLink bind(InetAddress bindAddress, int port, int maxPacketSize) throws IOException {
    DatagramSocket socket = new DatagramSocket(new InetSocketAddress(bindAddress, port));
    return new FairLossLink(socket, maxPacketSize);
  }

  public static FairLossLink unbound(int maxPacketSize) throws IOException {
    DatagramSocket socket = new DatagramSocket();
    return new FairLossLink(socket, maxPacketSize);
  }

  public void send(byte[] payload, InetAddress remoteIp, int remotePort) throws IOException {
    ValidationUtils.requireAllNonNull(named("payload", payload), named("remoteIp", remoteIp));
    ValidationUtils.requireValidPort(remotePort, "remotePort");
    if (payload.length > maxPacketSize) {
      throw new IOException("Serialized payload exceeds maxPacketSize (%d > %d)".formatted(payload.length, maxPacketSize));
    }

    DatagramPacket datagram = new DatagramPacket(payload, payload.length, remoteIp, remotePort);
    synchronized (sendLock) {
      socket.send(datagram);
    }
  }

  public InboundDatagram receive() throws IOException {
    synchronized (receiveLock) {
      receivePacket.setData(receiveBuffer, 0, receiveBuffer.length);
      receivePacket.setLength(receiveBuffer.length);
      socket.receive(receivePacket);

      byte[] payload = Arrays.copyOf(receiveBuffer, receivePacket.getLength());
      
      return new InboundDatagram(payload, receivePacket.getAddress(), receivePacket.getPort());
    }
  }

  @Override
  public void close() {
    socket.close();
  }
}

