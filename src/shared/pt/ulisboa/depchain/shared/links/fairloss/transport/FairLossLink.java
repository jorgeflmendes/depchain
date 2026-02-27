package pt.ulisboa.depchain.shared.links.fairloss.transport;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Objects;

import pt.ulisboa.depchain.shared.links.fairloss.codec.DpchCodec;
import pt.ulisboa.depchain.shared.links.fairloss.message.Dpch;

public final class FairLossLink implements AutoCloseable {
  // The underlying DatagramSocket used for sending and receiving packets.
  private final DatagramSocket socket;

  // Lock object to synchronize send operations, since DatagramSocket is not thread-safe for concurrent sends.
  private final Object sendLock = new Object();

  // Configuration parameters for the link.
  private final int responseTimeoutMs;

  // The maximum allowed size of the serialized packet payload (excluding UDP/IP headers).
  private final int maxPacketSize;

  private FairLossLink(DatagramSocket socket, int responseTimeoutMs, int maxPacketSize) throws SocketException {
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

  // Create and bind a socket to the given local address and port.
  public static FairLossLink bind(InetAddress bindAddress, int port, int responseTimeoutMs, int maxPacketSize) throws IOException {
    DatagramSocket socket = new DatagramSocket(new InetSocketAddress(bindAddress, port));
    return new FairLossLink(socket, responseTimeoutMs, maxPacketSize);
  }

  // Create a socket that is not bound to any local port.
  public static FairLossLink unbound(int responseTimeoutMs, int maxPacketSize) throws IOException {
    DatagramSocket socket = new DatagramSocket();
    return new FairLossLink(socket, responseTimeoutMs, maxPacketSize);
  }

  // Send one packet and wait for a reply with matching connection/sequence numbers.
  public Dpch sendRequest(Dpch request, InetAddress targetIp, int targetPort) throws IOException {
    Objects.requireNonNull(request, "request cannot be null");
    Objects.requireNonNull(targetIp, "targetIp cannot be null");

    // Send the request message to the target address and port.
    sendPacket(request, targetIp, targetPort);
    long deadlineMillis = System.currentTimeMillis() + responseTimeoutMs;

    try {
      // Wait for responses until we receive a matching one or timeout occurs.
      while (true) {
        long remainingMillis = deadlineMillis - System.currentTimeMillis();
        if (remainingMillis <= 0) {
          throw timeoutExceptionFor(request);
        }

        socket.setSoTimeout((int) Math.min(Integer.MAX_VALUE, remainingMillis));
        
        try {
          DatagramPacket packet = receivePacket();
          Dpch decoded = decodePacketOrNull(packet);
          if (decoded == null) {
            continue;
          }

          boolean sameConnectionId = decoded.connectionId() == request.connectionId();
          boolean sameSequence = decoded.sequenceNumber() == request.sequenceNumber();
          if (sameConnectionId && sameSequence) {
            return decoded;
          }
        } catch (SocketTimeoutException timeout) {
          throw timeoutExceptionFor(request);
        }
      }
    } finally {
      socket.setSoTimeout(0);
    }
  }

  // Wait until a request is received (blocking call).
  public InboundRequest receiveRequest() throws IOException {
    while (true) {
      DatagramPacket packet = receivePacket();
      Dpch decoded = decodePacketOrNull(packet);
      if (decoded != null) {
        return new InboundRequest(decoded, packet.getAddress(), packet.getPort());
      }
    }
  }

  // Send a response to the given target address and port.
  public void sendResponse(Dpch response, InetAddress targetIp, int targetPort)
      throws IOException {
    Objects.requireNonNull(response, "response cannot be null");
    Objects.requireNonNull(targetIp, "targetIp cannot be null");
    sendPacket(response, targetIp, targetPort);
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

  private static SocketTimeoutException timeoutExceptionFor(Dpch request) {
    return new SocketTimeoutException(
        "Timeout waiting for reply conn=%d seq=%d"
            .formatted(request.connectionId(), request.sequenceNumber()));
  }

  @Override
  public void close() {
    socket.close();
  }
}

