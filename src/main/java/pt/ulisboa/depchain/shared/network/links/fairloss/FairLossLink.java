package pt.ulisboa.depchain.shared.network.links.fairloss;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import org.eclipse.jdt.annotation.Nullable;

import pt.ulisboa.depchain.shared.network.links.BlockingLink;
import pt.ulisboa.depchain.shared.validation.ValidationUtils;

public final class FairLossLink implements BlockingLink<InboundBytes> {
  public static final int MAX_PACKET_SIZE = 8_192;
  public static final String DROP_PROBABILITY_PROPERTY = "depchain.fairloss.dropProbability";
  public static final String DUPLICATE_PROBABILITY_PROPERTY = "depchain.fairloss.duplicateProbability";
  public static final String ASYNC_MAX_DELAY_MS_PROPERTY = "depchain.fairloss.asyncMaxDelayMs";
  private static final int RECEIVE_BUFFER_RING_SIZE = 32;

  private final DatagramSocket socket;
  private final double simulatedDropProbability;
  private final double simulatedDuplicateProbability;
  private final int simulatedAsyncMaxDelayMs;
  private final byte[][] receiveBufferRing;
  private final DatagramPacket receivePacket;
  private final ThreadLocal<DatagramPacket> sendPacketByThread;
  private int configuredSoTimeoutMs;
  private int nextReceiveBufferIndex;

  private FairLossLink(DatagramSocket socket) throws SocketException {
    this.socket = ValidationUtils.requireNonNull(socket, "socket");
    this.simulatedDropProbability = configuredDropProbability();
    this.simulatedDuplicateProbability = configuredDuplicateProbability();
    this.simulatedAsyncMaxDelayMs = configuredAsyncMaxDelayMs();
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

    if (shouldDropOutboundPacket()) {
      return;
    }

    dispatchOutboundPacket(Arrays.copyOf(payload, payload.length), remoteEndpoint);
    if (shouldDuplicateOutboundPacket()) {
      dispatchOutboundPacket(Arrays.copyOf(payload, payload.length), remoteEndpoint);
    }
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

  private static double configuredDropProbability() {
    String configuredValue = System.getProperty(DROP_PROBABILITY_PROPERTY);
    if (configuredValue == null || configuredValue.isBlank()) {
      return 0.0d;
    }

    double parsedValue = Double.parseDouble(configuredValue.trim());
    if (parsedValue < 0.0d || parsedValue > 1.0d) {
      throw new IllegalArgumentException(DROP_PROBABILITY_PROPERTY + " must be between 0.0 and 1.0");
    }
    return parsedValue;
  }

  private boolean shouldDropOutboundPacket() {
    return simulatedDropProbability > 0.0d && ThreadLocalRandom.current().nextDouble() < simulatedDropProbability;
  }

  private boolean shouldDuplicateOutboundPacket() {
    return simulatedDuplicateProbability > 0.0d && ThreadLocalRandom.current().nextDouble() < simulatedDuplicateProbability;
  }

  private void dispatchOutboundPacket(byte[] payload, InetSocketAddress remoteEndpoint) throws IOException {
    if (simulatedAsyncMaxDelayMs <= 0) {
      sendDatagram(payload, remoteEndpoint);
      return;
    }

    int delayMs = ThreadLocalRandom.current().nextInt(simulatedAsyncMaxDelayMs + 1);
    Thread.startVirtualThread(() -> {
      try {
        if (delayMs > 0) {
          Thread.sleep(delayMs);
        }
        sendDatagram(payload, remoteEndpoint);
      } catch (Exception ignored) {
      }
    });
  }

  private void sendDatagram(byte[] payload, InetSocketAddress remoteEndpoint) throws IOException {
    DatagramPacket datagram = sendPacketByThread.get();
    datagram.setData(payload, 0, payload.length);
    datagram.setSocketAddress(remoteEndpoint);
    synchronized (socket) {
      socket.send(datagram);
    }
  }

  private static double configuredDuplicateProbability() {
    String configuredValue = System.getProperty(DUPLICATE_PROBABILITY_PROPERTY);
    if (configuredValue == null || configuredValue.isBlank()) {
      return 0.0d;
    }

    double parsedValue = Double.parseDouble(configuredValue.trim());
    if (parsedValue < 0.0d || parsedValue > 1.0d) {
      throw new IllegalArgumentException(DUPLICATE_PROBABILITY_PROPERTY + " must be between 0.0 and 1.0");
    }
    return parsedValue;
  }

  private static int configuredAsyncMaxDelayMs() {
    String configuredValue = System.getProperty(ASYNC_MAX_DELAY_MS_PROPERTY);
    if (configuredValue == null || configuredValue.isBlank()) {
      return 0;
    }

    int parsedValue = Integer.parseInt(configuredValue.trim());
    if (parsedValue < 0) {
      throw new IllegalArgumentException(ASYNC_MAX_DELAY_MS_PROPERTY + " must be >= 0");
    }
    return parsedValue;
  }
}
