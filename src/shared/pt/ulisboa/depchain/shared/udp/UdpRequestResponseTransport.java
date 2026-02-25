package pt.ulisboa.depchain.shared.udp;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Objects;

import pt.ulisboa.depchain.shared.udp.messages.MessageRequest;
import pt.ulisboa.depchain.shared.udp.messages.MessageResponse;

public final class UdpRequestResponseTransport implements AutoCloseable {
  private final DatagramSocket socket;
  private final Object sendLock = new Object();
  private final int responseTimeoutMs;
  private final int maxPacketSize;

  private UdpRequestResponseTransport(DatagramSocket socket, int responseTimeoutMs, int maxPacketSize) throws SocketException {
    this.socket = Objects.requireNonNull(socket, "socket cannot be null");

    if (responseTimeoutMs <= 0) {
      throw new IllegalArgumentException("responseTimeoutMs must be > 0");
    }

    if (maxPacketSize <= 0) {
      throw new IllegalArgumentException("maxPacketSize must be > 0");
    }
    
    this.responseTimeoutMs = responseTimeoutMs;
    this.maxPacketSize = maxPacketSize;
    this.socket.setSoTimeout(0);
  }

  // creates and binds a socket to the given local address and port (to receive incoming requests)
  public static UdpRequestResponseTransport bind(InetAddress bindAddress, int port, int responseTimeoutMs, int maxPacketSize) throws IOException {
    DatagramSocket socket = new DatagramSocket(new InetSocketAddress(bindAddress, port));
    return new UdpRequestResponseTransport(socket, responseTimeoutMs, maxPacketSize);
  }

  // creates a socket that is not bound to any local port (to send requests and receive responses, without needing to receive incoming requests)
  public static UdpRequestResponseTransport unbound(int responseTimeoutMs, int maxPacketSize) throws IOException {
    DatagramSocket socket = new DatagramSocket();
    return new UdpRequestResponseTransport(socket, responseTimeoutMs, maxPacketSize);
  }

  // wait until a response is received or the configured timeout elapses
  public MessageResponse sendRequest(MessageRequest request, InetAddress targetIp, int targetPort) throws IOException {
    Objects.requireNonNull(request, "request cannot be null");
    Objects.requireNonNull(targetIp, "targetIp cannot be null");

    sendObject(request, targetIp, targetPort);

    long deadlineMillis = System.currentTimeMillis() + responseTimeoutMs;

    try {
      while (true) {
        long remainingMillis = deadlineMillis - System.currentTimeMillis();
        if (remainingMillis <= 0) {
          return new MessageResponse(request.requestId(), false, "Timeout waiting for response");
        }

        socket.setSoTimeout((int) Math.min(Integer.MAX_VALUE, remainingMillis));
        try {
          DatagramPacket packet = receivePacket();
          Object decoded = decodePacketOrNull(packet);
          if (!(decoded instanceof MessageResponse candidate)) {
            continue;
          }

          // only complete this call when the response carries the same requestId as the original request
          if (request.requestId().equals(candidate.requestId())) {
            return candidate;
          }
        } catch (SocketTimeoutException timeout) {
          return new MessageResponse(request.requestId(), false, "Timeout waiting for response");
        }
      }
    } finally {
      socket.setSoTimeout(0);
    }
  }

  // wait until a request is received (blocking call)
  public MessageRequest receiveRequest() throws IOException {
    while (true) {
      DatagramPacket packet = receivePacket();
      Object decoded = decodePacketOrNull(packet);
      if (decoded instanceof MessageRequest request) {
        return request.withSender(packet.getAddress(), packet.getPort());
      }
    }
  }

  // send a response to the given target address and port
  public void sendResponse(MessageResponse response, InetAddress targetIp, int targetPort) throws IOException {
    Objects.requireNonNull(response, "response cannot be null");
    Objects.requireNonNull(targetIp, "targetIp cannot be null");

    sendObject(response, targetIp, targetPort);
  }

  @Override
  public void close() {
    socket.close();
  }

  private void sendObject(Object value, InetAddress targetIp, int targetPort) throws IOException {
    byte[] payload = SerializationUtils.toBytes(value);
    DatagramPacket packet = new DatagramPacket(payload, payload.length, targetIp, targetPort);
    synchronized (sendLock) {
      socket.send(packet);
    }
  }

  private DatagramPacket receivePacket() throws IOException {
    DatagramPacket packet = new DatagramPacket(new byte[maxPacketSize], maxPacketSize);
    socket.receive(packet);
    return packet;
  }

  private Object decodePacketOrNull(DatagramPacket packet) {
    try {
      return SerializationUtils.fromBytes(packet.getData(), packet.getOffset(), packet.getLength());
    } catch (IOException | ClassNotFoundException ignored) {
      return null;
    }
  }
}
