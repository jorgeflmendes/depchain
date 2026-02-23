package pt.ulisboa.depchain.shared.udp;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public final class DatagramTransport implements AutoCloseable {
  private final DatagramSocket socket;

  private DatagramTransport(DatagramSocket socket, int timeoutMs) throws IOException {
    this.socket = socket;
    this.socket.setSoTimeout(timeoutMs);
  }

  public static DatagramTransport bind(InetAddress bindAddress, int port, int timeoutMs) throws IOException {
    DatagramSocket socket = new DatagramSocket(new InetSocketAddress(bindAddress, port));
    return new DatagramTransport(socket, timeoutMs);
  }

  public static DatagramTransport unbound(int timeoutMs) throws IOException {
    DatagramSocket socket = new DatagramSocket();
    return new DatagramTransport(socket, timeoutMs);
  }

  public int localPort() {
    return socket.getLocalPort();
  }

  public void send(String host, int port, byte[] payload) throws IOException {
    DatagramPacket packet = new DatagramPacket(payload, payload.length, new InetSocketAddress(host, port));
    socket.send(packet);
  }

  public void send(InetAddress address, int port, byte[] payload) throws IOException {
    DatagramPacket packet = new DatagramPacket(payload, payload.length, address, port);
    socket.send(packet);
  }

  public ReceivedDatagram receive(int maxPacketSize) throws IOException {
    byte[] buffer = new byte[maxPacketSize];
    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
    socket.receive(packet);

    byte[] payload = new byte[packet.getLength()];
    System.arraycopy(packet.getData(), packet.getOffset(), payload, 0, packet.getLength());
    return new ReceivedDatagram(payload, packet.getAddress(), packet.getPort());
  }

  @Override
  public void close() {
    socket.close();
  }

  public record ReceivedDatagram(byte[] payload, InetAddress address, int port) {
  }
}
