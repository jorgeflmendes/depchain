package pt.ulisboa.depchain.shared.network.links.perfect;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;
import static pt.ulisboa.depchain.shared.utils.ValidationUtils.requireAllNonNull;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.dpch.DpchSerialization;
import pt.ulisboa.depchain.shared.network.dpch.DpchType;
import pt.ulisboa.depchain.shared.network.links.fairloss.InboundDatagram;
import pt.ulisboa.depchain.shared.network.links.perfect.state.ReceiverState;
import pt.ulisboa.depchain.shared.network.links.perfect.state.SenderState;
import pt.ulisboa.depchain.shared.network.links.stubborn.StubbornLink;
import pt.ulisboa.depchain.shared.network.links.stubborn.model.TrackedMessage;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundMessage;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public class PerfectLink implements AutoCloseable {
  private static final byte[] ACK_DATA = new byte[] {DpchType.DATA.code()};
  private static final byte[] ACK_SYN = new byte[] {DpchType.SYN.code()};
  private static final byte[] ACK_FIN = new byte[] {DpchType.FIN.code()};

  // Configuration parameters for the perfect link, with validation in the constructor.
  public record Config(int maxWindowSize, int maxStreamStates, long streamIdleTtlMs) {
    public Config {
      maxWindowSize = ValidationUtils.requirePositiveInt(maxWindowSize, "maxWindowSize");
      maxStreamStates = ValidationUtils.requirePositiveInt(maxStreamStates, "maxStreamStates");
      streamIdleTtlMs = ValidationUtils.requirePositiveLong(streamIdleTtlMs, "streamIdleTtlMs");
    }
  }

  public record BuildConfig(int maxPacketSize, StubbornLink.Config stubborn, Config perfect) {
    public BuildConfig {
      maxPacketSize = ValidationUtils.requirePositiveInt(maxPacketSize, "maxPacketSize");
      requireAllNonNull(named("stubborn", stubborn), named("perfect", perfect));
    }
  }

  private final StubbornLink stubbornLink;
  private final int maxWindowSize;
  private final int maxStreamStates;
  private final long streamIdleTtlMs;
  private final long cleanupCheckIntervalMs;
  private volatile long nextCleanupAtMs;

  private final Map<ConnectionKey, SenderState> sendSequences = new ConcurrentHashMap<>();
  private final Map<ConnectionKey, ReceiverState> receiverStates = new ConcurrentHashMap<>();
  private final Map<ConnectionKey, Integer> ttlEvictedReceiverDeliveredFloors = new ConcurrentHashMap<>();
  private final BlockingQueue<InboundMessage> deliveryQueue = new LinkedBlockingQueue<>();

  // Worker thread for managing incoming messages/acknowledgments/cleanup, with a flag to signal it should stop.
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final Thread workerThread;

  public PerfectLink(StubbornLink stubbornLink, int maxWindowSize, int maxStreamStates, long streamIdleTtlMs) {
    this.stubbornLink = ValidationUtils.requireNonNull(stubbornLink, "stubbornLink");
    this.maxWindowSize = ValidationUtils.requirePositiveInt(maxWindowSize, "maxWindowSize");
    this.maxStreamStates = ValidationUtils.requirePositiveInt(maxStreamStates, "maxStreamStates");
    this.streamIdleTtlMs = ValidationUtils.requirePositiveLong(streamIdleTtlMs, "streamIdleTtlMs");
    this.cleanupCheckIntervalMs = Math.max(250L, Math.min(this.streamIdleTtlMs, 5_000L)); // Prevents cleanup from running too often or too rarely.
    this.nextCleanupAtMs = System.currentTimeMillis() + cleanupCheckIntervalMs;

    this.workerThread = Thread.ofVirtual().name("perfect-link").start(this::runReceiveLoop);
  }

  public static PerfectLink bind(InetAddress bindAddress, int port, BuildConfig config) throws IOException {
    StubbornLink stubbornLink = StubbornLink.bind(bindAddress, port, config.maxPacketSize(), config.stubborn());
    Config perfect = config.perfect();
    return new PerfectLink(stubbornLink, perfect.maxWindowSize(), perfect.maxStreamStates(), perfect.streamIdleTtlMs());
  }

  public static PerfectLink unbound(BuildConfig config) throws IOException {
    StubbornLink stubbornLink = StubbornLink.unbound(config.maxPacketSize(), config.stubborn());
    Config perfect = config.perfect();
    return new PerfectLink(stubbornLink, perfect.maxWindowSize(), perfect.maxStreamStates(), perfect.streamIdleTtlMs());
  }

  // Send a packet (DATA/SYN/FIN) with the given parameters, optionally tracking it for acknowledgment. ACK packets must be sent via sendAck.
  public void send(long connectionId, DpchType packetType, boolean withAck, byte[] payload, InetAddress remoteIp, int remotePort) {
    switch (ValidationUtils.requireNonNull(packetType, "packetType")) {
      case ACK -> throw new IllegalArgumentException("ACK must be sent via sendAck");
      case DATA -> {
        if (withAck) {
          throw new IllegalArgumentException("DATA cannot be combined with ACK");
        }
        sendTracked(connectionId, packetType, false, payload, remoteIp, remotePort);
      }
      case SYN, FIN -> sendTracked(connectionId, packetType, withAck, payload, remoteIp, remotePort);
    }
  }

  // Send an ACK packet acknowledging the message with the given sequence number and type.
  public void sendAck(long connectionId, int acknowledgedSequence, DpchType acknowledgedType, InetAddress remoteIp, int remotePort) {
    sendAckIgnoringErrors(connectionId, acknowledgedSequence, new InetSocketAddress(remoteIp, remotePort), ackPayloadFor(acknowledgedType));
  }

  // Receive the next delivered message, blocking until one is available.
  public InboundMessage receive() throws InterruptedException {
    return deliveryQueue.take();
  }

  // Receive the next delivered message, blocking until one is available or the timeout elapses (in which case null is returned).
  public InboundMessage receive(long timeoutMs) throws InterruptedException {
    return deliveryQueue.poll(ValidationUtils.requireNonNegativeLong(timeoutMs, "timeoutMs"), TimeUnit.MILLISECONDS);
  }

  // Wait until there are no pending DATA messages for the given connection and remote, or until the timeout elapses.
  public boolean waitUntilDataDrained(long connectionId, InetAddress remoteIp, int remotePort, long timeoutMs) throws InterruptedException {
    return waitUntilTypeDrained(connectionId, remoteIp, remotePort, DpchType.DATA, timeoutMs);
  }

  // Wait until there are no pending messages of the given type for the given connection and remote, or until the timeout elapses.
  public boolean waitUntilTypeDrained(long connectionId, InetAddress remoteIp, int remotePort, DpchType packetType, long timeoutMs) throws InterruptedException {
    requireAllNonNull(named("remoteIp", remoteIp), named("packetType", packetType));
    if (packetType == DpchType.ACK) {
      throw new IllegalArgumentException("ACK packets are not tracked");
    }

    long deadlineMs = System.currentTimeMillis() + ValidationUtils.requireNonNegativeLong(timeoutMs, "timeoutMs");
    if (!running.get()) {
      return false;
    }

    SenderState senderState = sendSequences.get(ConnectionKey.from(remoteIp, remotePort, connectionId));
    return senderState == null || senderState.waitUntilNoPending(packetType, deadlineMs);
  }

  // Computes a safe upper bound to wait for tracked DATA retries/acks before giving up.
  public long trackedDrainTimeoutMs() {
    long retransmitBudgetMs = stubbornLink.baseDelayMs() + stubbornLink.maxDelayMs();
    long trackedLifetimeMs = stubbornLink.maxTrackedLifetimeMs();
    long boundedByLifetime;
    if (trackedLifetimeMs < 0L) { // If lifetime is unlimited (<0), use retransmit budget only.
      boundedByLifetime = retransmitBudgetMs;
    } else {
      boundedByLifetime = Math.min(retransmitBudgetMs, trackedLifetimeMs);
    }
    return Math.max(1L, Math.min(streamIdleTtlMs, boundedByLifetime));
  }

  // Assigns per-stream sequence number and registers the packet in StubbornLink tracking.
  private void sendTracked(long connectionId, DpchType type, boolean withAck, byte[] payload, InetAddress remoteIp, int remotePort) {
    ConnectionKey key = ConnectionKey.from(remoteIp, remotePort, connectionId);
    long now = System.currentTimeMillis();

    // For control packets, keep one in-flight sequence per type so retries/copies map to a single ACK cancellation key.
    boolean reusePendingControl = (type == DpchType.SYN || type == DpchType.FIN);
    int seq = sendSequences.computeIfAbsent(key, ignored -> new SenderState()).nextOrPendingSequence(type, reusePendingControl, now); // Control retransmits reuse existing in-flight sequence to avoid duplicated handshake tracks.
    
    Dpch packet = buildTrackedPacket(connectionId, type, withAck, seq, payload);
    stubbornLink.sendTracked(new TrackedMessage.Key(connectionId, seq, Byte.toUnsignedInt(type.code())), serializeUnchecked(packet), remoteIp, remotePort); // Tracking key must match ACK tuple used in handleAck.
    cleanStaleStatesIfNeeded(now);
  }

  // Centralizes packet creation so tracked send path always uses the same DPCH shape.
  private static Dpch buildTrackedPacket(long connectionId, DpchType type, boolean withAck, int sequenceNumber, byte[] payload) {
    return Dpch.from(connectionId, type, withAck, sequenceNumber, payload);
  }

  private void runReceiveLoop() {
    while (running.get()) {
      try {
        processInbound(decodeInbound(stubbornLink.receive()));
      } catch (IOException | RuntimeException exception) {
        if (!running.get()) {
          break;
        }
        System.err.println("PerfectLink worker error: " + exception.getMessage());
      }
    }
  }
  
  // Process an inbound message by handling ACKs and reliable packets, and performing cleanup if needed.
  private void processInbound(InboundMessage inbound) {
    Dpch packet = inbound.packet();
    InetSocketAddress remote = new InetSocketAddress(inbound.senderIp(), inbound.senderPort());
    ConnectionKey key = ConnectionKey.from(remote, packet.connectionId());
    long now = System.currentTimeMillis();

    DpchType reliableType = packet.reliableTypeOrNull();
    if (packet.hasType(DpchType.ACK)) {
      handleAck(packet, reliableType, key, remote, now);
    }
    if (reliableType != null) {
      handleReliable(inbound, packet, reliableType, key, now);
    }
    cleanStaleStatesIfNeeded(now);
  }

  // Cancels matching tracked retransmissions when a valid ACK is received.
  private void handleAck(Dpch ackPacket, DpchType fallbackAcknowledgedType, ConnectionKey key, InetSocketAddress remote, long now) {
    SenderState senderState = sendSequences.get(key);
    if (senderState == null) {
      return;
    }

    DpchType acknowledgedType = decodeAcknowledgedType(ackPacket.payload(), fallbackAcknowledgedType);
    int acknowledgedSequence = ackPacket.sequenceNumber();
    if (acknowledgedType == null || acknowledgedSequence < 0) {
      return;
    }

    List<TrackedMessage.Key> cancellations = senderState.acknowledge(ackPacket.connectionId(), acknowledgedSequence, acknowledgedType, now);
    for (TrackedMessage.Key trackedKey : cancellations) {
      stubbornLink.cancelTracked(trackedKey, remote.getAddress(), remote.getPort());
    }
  }

  // Deduplicates and reorders DATA/SYN/FIN before exposing messages to upper layers.
  private void handleReliable(InboundMessage inbound, Dpch packet, DpchType reliableType, ConnectionKey key, long now) {
    ReceiverState state = receiverStates.computeIfAbsent(key, ignored -> {Integer persisted = ttlEvictedReceiverDeliveredFloors.remove(key); if (persisted == null) {return new ReceiverState(0);} return new ReceiverState(persisted);}); // Restores delivered floor if state was TTL-evicted.

    boolean shouldAckData = false;
    synchronized (state) {
      state.touch(now);
      int seq = packet.sequenceNumber();
      if (seq < 0) {
        return;
      }

      if (state.isAlreadyDelivered(seq)) { // Duplicate: DATA gets ACKed again; control (SYN/FIN) is re-delivered.
        if (reliableType == DpchType.DATA) {
          shouldAckData = true;
        } else {
          deliveryQueue.offer(inbound);
        }
      } else if (!state.isOutsideWindow(seq, maxWindowSize)) { // Only buffers packets inside the sliding window.
        if (reliableType == DpchType.DATA) {
          shouldAckData = true;
        }
        if (state.bufferIfNew(seq, inbound)) {
          while (state.hasNextInOrderReady()) {
            deliveryQueue.offer(state.pollNextInOrder());
          }
        }
      }
    }

    if (shouldAckData) {
      sendAckIgnoringErrors(packet.connectionId(), packet.sequenceNumber(), new InetSocketAddress(inbound.senderIp(), inbound.senderPort()), ACK_DATA);
    }
  }

  // Best-effort ACK send: receive path must not fail due to transient network send errors.
  private void sendAckIgnoringErrors(long connectionId, int sequenceNumber, InetSocketAddress remote, byte[] ackPayload) {
    try {
      stubbornLink.sendOnce(serializeUnchecked(Dpch.from(connectionId, DpchType.ACK, sequenceNumber, ackPayload)), remote.getAddress(), remote.getPort());
    } catch (IOException exception) {
      System.err.printf("PerfectLink ACK send error to %s:%d for conn=%s seq=%d = %s%n", remote.getAddress().getHostAddress(), remote.getPort(), connectionId, sequenceNumber, exception.getMessage());
    }
  }

  // Periodically or on pressure, evicts stale sender/receiver states to keep maps bounded.
  private void cleanStaleStatesIfNeeded(long now) {
    boolean overCapacity = sendSequences.size() > maxStreamStates || receiverStates.size() > maxStreamStates || ttlEvictedReceiverDeliveredFloors.size() > maxStreamStates; // Also trigger cleanup early when over capacity.
    if (!overCapacity && now < nextCleanupAtMs) {
      return;
    }
    
    nextCleanupAtMs = now + cleanupCheckIntervalMs;

    sendSequences.entrySet().removeIf(entry -> {
      SenderState state = entry.getValue();
      if (!state.isStale(now, streamIdleTtlMs)) {
        return false;
      }

      synchronized (state) {
        state.notifyAll();
      }

      return true;
    });

    receiverStates.entrySet().removeIf(entry -> {
      ReceiverState state = entry.getValue();
      if (!state.isStale(now, streamIdleTtlMs)) {
        return false;
      }

      synchronized (state) {
        ttlEvictedReceiverDeliveredFloors.merge(entry.getKey(), state.nextExpectedSequence(), Math::max); // Keep highest nextExpected to avoid dedup regression after recreation.
      }

      return true;
    });

    compactDeliveredFloorsIfNeeded();
  }

  // Shrinks persisted delivered-floor cache, preferring entries not tied to active streams.
  private void compactDeliveredFloorsIfNeeded() {
    if (ttlEvictedReceiverDeliveredFloors.size() <= maxStreamStates) {
      return;
    }

    ttlEvictedReceiverDeliveredFloors.entrySet().removeIf(entry -> {
      if (ttlEvictedReceiverDeliveredFloors.size() <= maxStreamStates) { // Re-check size after each candidate removal.
        return false;
      }
      ConnectionKey key = entry.getKey();
      return !receiverStates.containsKey(key) && !sendSequences.containsKey(key); // Remove only floors without active sender/receiver state.
    });
  }

  // Encodes which reliable type (DATA/SYN/FIN) this ACK refers to.
  private static byte[] ackPayloadFor(DpchType type) {
    return switch (type) {
      case DATA -> ACK_DATA;
      case SYN -> ACK_SYN;
      case FIN -> ACK_FIN;
      case ACK -> throw new IllegalArgumentException("ACK cannot acknowledge ACK");
    };
  }

  // Extracts acknowledged reliable type from ACK payload, with fallback for legacy zero-length ACKs.
  private static DpchType decodeAcknowledgedType(byte[] ackPayload, DpchType fallback) {
    if (ackPayload.length == 0) { // ACKs without explicit type in payload.
      return fallback;
    }
    
    if (ackPayload.length != 1) { // ACK payload must carry exactly one type byte.
      return null;
    }

    try {
      DpchType decoded = DpchType.fromCode(ackPayload[0]);
      if (decoded == DpchType.DATA || decoded == DpchType.SYN || decoded == DpchType.FIN) {
        return decoded;
      }

      return null;
    } catch (IllegalArgumentException ignored) {
      return null;
    }
  }

  // Converts serialization failures into unchecked exceptions for internal send paths.
  private static byte[] serializeUnchecked(Dpch packet) {
    try {
      return DpchSerialization.toBytes(packet);
    } catch (IOException exception) {
      throw new IllegalStateException("Failed to serialize DPCH packet", exception);
    }
  }

  // Parses raw datagram payload into a typed inbound message with sender metadata.
  private static InboundMessage decodeInbound(InboundDatagram datagram) throws IOException {
    byte[] payload = datagram.payload();
    Dpch packet = DpchSerialization.fromBytes(payload, 0, payload.length);
    return new InboundMessage(packet, datagram.senderIp(), datagram.senderPort());
  }

  @Override
  public void close() throws Exception {
    if (!running.compareAndSet(true, false)) {
      return;
    }

    stubbornLink.close();
    workerThread.interrupt();
    workerThread.join(2_000L);
  }
}
