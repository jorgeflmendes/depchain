package pt.ulisboa.depchain.shared.network.links.perfect;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.dpch.DpchType;
import pt.ulisboa.depchain.shared.network.links.stubborn.StubbornLink;
import pt.ulisboa.depchain.shared.network.links.stubborn.TrackedMessage;
import pt.ulisboa.depchain.shared.network.model.EndpointConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundMessage;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public class PerfectLink implements AutoCloseable {
  // Perfect-link receive/reorder configuration.
  public record Config(int maxWindowSize, int maxStreamStates, long streamIdleTtlMs) {
    public Config {
      maxWindowSize = ValidationUtils.requirePositiveInt(maxWindowSize, "maxWindowSize");
      maxStreamStates = ValidationUtils.requirePositiveInt(maxStreamStates, "maxStreamStates");
      streamIdleTtlMs = ValidationUtils.requirePositiveLong(streamIdleTtlMs, "streamIdleTtlMs");
    }
  }

  // Full transport configuration needed to build a PerfectLink.
  public record BuildConfig(int maxPacketSize, StubbornLink.Config stubborn, Config perfect) {
    public BuildConfig {
      maxPacketSize = ValidationUtils.requirePositiveInt(maxPacketSize, "maxPacketSize");
      if (stubborn == null) {
        throw new NullPointerException("stubborn cannot be null");
      }
      if (perfect == null) {
        throw new NullPointerException("perfect cannot be null");
      }
    }
  }

  // Lower layer link that provides best-effort delivery with tracking capabilities.
  private final StubbornLink stubbornLink;
  
  // Maximum number of out-of-order packets accepted per stream.
  private final int maxWindowSize;

  // To avoid unbounded map growth.
  private final int maxStreamStates;
  private final long streamIdleTtlMs;

  // Send and receive state management for each endpoint-connection pair.
  private final Map<EndpointConnectionKey, SenderState> sendSequences;
  private final Map<EndpointConnectionKey, ReceiverState> receiverStates;
  
  // Backup floor per stream used only after receiver-state removal (for active streams, the authoritative floor lives in ReceiverState.nextExpectedSeq)
  private final Map<EndpointConnectionKey, Integer> ttlEvictedReceiverDeliveredFloors;

  // Delivery queue for in-order messages ready to be delivered to the application layer.
  private final BlockingQueue<InboundMessage> deliveryQueue;
  
  // Worker lifecycle and maintenance.
  private final AtomicBoolean running;
  private final Thread workerThread;

  public PerfectLink(StubbornLink stubbornLink, int maxWindowSize, int maxStreamStates, long streamIdleTtlMs) {
    this.stubbornLink = stubbornLink;
    this.maxWindowSize = ValidationUtils.requirePositiveInt(maxWindowSize, "maxWindowSize");
    this.maxStreamStates = ValidationUtils.requirePositiveInt(maxStreamStates, "maxStreamStates");
    this.streamIdleTtlMs = ValidationUtils.requirePositiveLong(streamIdleTtlMs, "streamIdleTtlMs");

    this.sendSequences = new ConcurrentHashMap<>();
    this.receiverStates = new ConcurrentHashMap<>();
    this.ttlEvictedReceiverDeliveredFloors = new ConcurrentHashMap<>();
    this.deliveryQueue = new LinkedBlockingQueue<>();
    this.running = new AtomicBoolean(true);

    this.workerThread = Thread.ofVirtual().name("perfect-link").start(this::runReceiveLoop);
  }

  // Build a PerfectLink bound to a local address/port.
  public static PerfectLink bind(InetAddress bindAddress, int port, BuildConfig config) throws IOException {
    StubbornLink stubbornLink = StubbornLink.bind(bindAddress, port, config.maxPacketSize(), config.stubborn());
    Config perfect = config.perfect();
    return new PerfectLink(stubbornLink, perfect.maxWindowSize(), perfect.maxStreamStates(), perfect.streamIdleTtlMs());
  }

  // Build a PerfectLink with an ephemeral local socket.
  public static PerfectLink unbound(BuildConfig config) throws IOException {
    StubbornLink stubbornLink = StubbornLink.unbound(config.maxPacketSize(), config.stubborn());
    Config perfect = config.perfect();
    return new PerfectLink(stubbornLink, perfect.maxWindowSize(), perfect.maxStreamStates(), perfect.streamIdleTtlMs());
  }

  // Send a message reliably to the specified remote endpoint and connection ID.
  public void sendReliable(UUID connectionId, byte[] payload, InetAddress remoteIp, int remotePort) {
    sendByType(connectionId, DpchType.DATA, payload, remoteIp, remotePort);
  }

  // SYN message to initiate a new connection.
  public void sendSyn(UUID connectionId, byte[] payload, InetAddress remoteIp, int remotePort) {
    sendByType(connectionId, DpchType.SYN, payload, remoteIp, remotePort);
  }

  // FIN message to terminate a connection.
  public void sendFin(UUID connectionId, byte[] payload, InetAddress remoteIp, int remotePort) {
    sendByType(connectionId, DpchType.FIN, payload, remoteIp, remotePort);
  }

  // Common send logic for different reliable packet types.
  private void sendByType(UUID connectionId, DpchType type, byte[] payload, InetAddress remoteIp, int remotePort) {
    EndpointConnectionKey key = connectionKey(connectionId, remoteIp, remotePort);
    long now = System.currentTimeMillis();
    SenderState senderState = sendSequences.computeIfAbsent(key, ignored -> new SenderState());
    int seq = senderState.nextSequence(type, now);

    Dpch packet = new Dpch(connectionId, type, seq, payload);
    stubbornLink.sendTracked(packet, remoteIp, remotePort);
    cleanStaleStatesIfNeeded();
  }

  // Blocking call to receive the next in-order message ready for delivery to the application layer.
  public InboundMessage receive() throws InterruptedException {
    return deliveryQueue.take();
  }

  // Poll for a message with a timeout, returning null if no message is available within the specified time.
  public InboundMessage receive(long timeoutMs) throws InterruptedException {
    long sanitizedTimeoutMs = ValidationUtils.requireNonNegativeLong(timeoutMs, "timeoutMs");
    return deliveryQueue.poll(sanitizedTimeoutMs, TimeUnit.MILLISECONDS);
  }

  // Main loop that continuously receives messages from the stubborn link and processes them until the PerfectLink is closed.
  private void runReceiveLoop() {
    while (running.get()) {
      try {
        processInbound(stubbornLink.receive());  // Waits for the next message from the stubborn link.
      } catch (Exception exception) {
        if (!running.get()) {
          break;
        }

        System.err.println("PerfectLink worker error: " + exception.getMessage());
      }
    }
  }

  // Process incoming messages
  private void processInbound(InboundMessage inbound) {
    Dpch packet = inbound.packet();
    InetSocketAddress remote = new InetSocketAddress(inbound.senderIp(), inbound.senderPort());
    EndpointConnectionKey key = connectionKey(packet.connectionId(), remote);
    long now = System.currentTimeMillis();

    if (packet.type() == DpchType.ACK) {
      handleAck(packet, key, remote, now);
    } else if (packet.type() == DpchType.DATA || packet.type() == DpchType.SYN || packet.type() == DpchType.FIN) {
      handleReliable(inbound, packet, key, remote, now);
    }

    cleanStaleStatesIfNeeded();
  }

  // Handle ACK messages by canceling matching tracked packets.
  private void handleAck(Dpch ackPacket, EndpointConnectionKey key, InetSocketAddress remote, long now) {
    SenderState senderState = sendSequences.get(key);
    if (senderState == null) {
      return;
    }

    DpchType acknowledgedType = decodeAcknowledgedType(ackPacket.payload());
    if (acknowledgedType == null) {
      return;
    }

    int acknowledgedSequence = ackPacket.sequenceNumber();
    if (acknowledgedSequence < 0) {
      return;
    }

    List<TrackedMessage.Key> cancellations = senderState.acknowledge(ackPacket.connectionId(), acknowledgedSequence, acknowledgedType, now);
    for (TrackedMessage.Key trackedKey : cancellations) {
      stubbornLink.cancelTracked(trackedKey, remote.getAddress(), remote.getPort());
    }
  }

  // Handle reliable packets (DATA/SYN/FIN) with deduplication + in-order delivery.
  private void handleReliable(InboundMessage inbound, Dpch packet, EndpointConnectionKey key, InetSocketAddress remote, long now) {
    ReceiverState state = receiverStates.computeIfAbsent(key, ignored -> {
      // This stream became active again.
      Integer persisted = ttlEvictedReceiverDeliveredFloors.remove(key);
      int restoredFloor = (persisted == null) ? 0 : persisted;
      return new ReceiverState(restoredFloor);
    });

    boolean shouldAck = false;
    synchronized (state) {
      state.touch(now);
      int seq = packet.sequenceNumber();
      if (seq < 0) {
        return;
      }

      if (state.isAlreadyDelivered(seq)) {
        shouldAck = true;
      } else if (state.isOutsideWindow(seq, maxWindowSize)) {
        return;
      } else {
        // Add packet only if not already buffered.
        boolean bufferedNow = state.bufferIfNew(seq, inbound);
        shouldAck = true;
        if (bufferedNow) {
          // Deliver in-order packets in cascade.
          while (state.hasNextInOrderReady()) {
            InboundMessage delivered = state.pollNextInOrder();
            deliveryQueue.offer(delivered);
          }
        }

        // FIN is acknowledged only after it becomes in-order delivered.
        if (packet.type() == DpchType.FIN) {
          shouldAck = state.isAlreadyDelivered(seq);
        }
      }
    }

    if (shouldAck) {
      sendAckIgnoringErrors(packet, remote);
    }
  }

  // Send an ACK for the received packet, ignoring any exceptions to avoid impacting the receive loop.
  private void sendAckIgnoringErrors(Dpch dataPacket, InetSocketAddress remote) {
    try {
      byte[] ackPayload = new byte[] {dataPacket.type().code()};
      stubbornLink.sendOnce(Dpch.ack(dataPacket.connectionId(), dataPacket.sequenceNumber(), ackPayload), remote.getAddress(), remote.getPort());
    } catch (IOException exception) {
      System.err.printf("PerfectLink ACK send error to %s:%d for conn=%s seq=%d = %s%n", remote.getAddress().getHostAddress(), remote.getPort(), dataPacket.connectionId(), dataPacket.sequenceNumber(), exception.getMessage());
    }
  }

  // To know the ack type (DATA/SYN/FIN).
  private DpchType decodeAcknowledgedType(byte[] ackPayload) {
    if (ackPayload.length != 1) {
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

  private EndpointConnectionKey connectionKey(UUID connectionId, InetAddress remoteIp, int remotePort) {
    return new EndpointConnectionKey(new InetSocketAddress(remoteIp, remotePort), connectionId);
  }

  private EndpointConnectionKey connectionKey(UUID connectionId, InetSocketAddress remoteEndpoint) {
    return new EndpointConnectionKey(remoteEndpoint, connectionId);
  }

  // Remove stale sender and receiver states that have been idle for too long.
  private void cleanStaleStatesIfNeeded() {
    if (sendSequences.size() <= maxStreamStates && receiverStates.size() <= maxStreamStates && ttlEvictedReceiverDeliveredFloors.size() <= maxStreamStates) {
      return;
    }
    
    long now = System.currentTimeMillis();
    sendSequences.entrySet().removeIf(entry -> entry.getValue().isStale(now, streamIdleTtlMs));

    receiverStates.entrySet().removeIf(entry -> {
      ReceiverState state = entry.getValue();
      if (!state.isStale(now, streamIdleTtlMs)) {
        return false;
      }

      synchronized (state) {
        // Persist the current floor only when receiver state is evicted.
        updateDeliveredFloor(entry.getKey(), state.nextExpectedSequence());
      }
      return true;
    });

    compactDeliveredFloorsIfNeeded();
  }

  // To keep the delivered floor info when rec. state is removed and allow it to be used if the stream becomes active again soon
  private void updateDeliveredFloor(EndpointConnectionKey key, int nextExpectedSequence) {
    ttlEvictedReceiverDeliveredFloors.merge(key, nextExpectedSequence, Math::max);
  }

  // Keep delivered floor table bounded. Prefer dropping entries for inactive streams.
  private void compactDeliveredFloorsIfNeeded() {
    if (ttlEvictedReceiverDeliveredFloors.size() <= maxStreamStates) {
      return;
    }

    ttlEvictedReceiverDeliveredFloors.entrySet().removeIf(entry -> {
      if (ttlEvictedReceiverDeliveredFloors.size() <= maxStreamStates) {
        return false;
      }

      EndpointConnectionKey key = entry.getKey();
      boolean keep = receiverStates.containsKey(key) || sendSequences.containsKey(key);
      if (keep) {
        return false;
      }

      return true;
    });
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
