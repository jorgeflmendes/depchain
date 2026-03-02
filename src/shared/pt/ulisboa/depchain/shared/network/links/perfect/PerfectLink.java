package pt.ulisboa.depchain.shared.network.links.perfect;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.dpch.DpchType;
import pt.ulisboa.depchain.shared.network.links.stubborn.StubbornLink;
import pt.ulisboa.depchain.shared.network.links.stubborn.TrackedMessage;
import pt.ulisboa.depchain.shared.network.model.EndpointConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundMessage;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public class PerfectLink implements AutoCloseable {
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
    this.deliveryQueue = new LinkedBlockingQueue<>();
    this.running = new AtomicBoolean(true);

    this.workerThread = Thread.ofVirtual().name("perfect-link").start(this::runReceiveLoop);
  }

  // Build a PerfectLink bound to a local address/port.
  public static PerfectLink bind(InetAddress bindIp, int port, int maxPacketSize, long baseDelayMs, long maxDelayMs, double jitterRatio, int maxPending, int heapCompactMinSize, int maxRetryAttempts, long maxTrackedLifetimeMs, int maxWindowSize, int maxStreamStates, long streamIdleTtlMs) throws IOException {
    StubbornLink stubbornLink = StubbornLink.bind(bindIp, port, maxPacketSize, baseDelayMs, maxDelayMs, jitterRatio, maxPending, heapCompactMinSize, maxRetryAttempts, maxTrackedLifetimeMs);
    return new PerfectLink(stubbornLink, maxWindowSize, maxStreamStates, streamIdleTtlMs);
  }

  // Build a PerfectLink with an ephemeral local socket.
  public static PerfectLink unbound(int maxPacketSize, long baseDelayMs, long maxDelayMs, double jitterRatio, int maxPending, int heapCompactMinSize, int maxRetryAttempts, long maxTrackedLifetimeMs, int maxWindowSize, int maxStreamStates, long streamIdleTtlMs) throws IOException {
    StubbornLink stubbornLink = StubbornLink.unbound(maxPacketSize, baseDelayMs, maxDelayMs, jitterRatio, maxPending, heapCompactMinSize, maxRetryAttempts, maxTrackedLifetimeMs);
    return new PerfectLink(stubbornLink, maxWindowSize, maxStreamStates, streamIdleTtlMs);
  }

  // Send a message reliably to the specified remote endpoint and connection ID.
  public void sendReliable(int connectionId, byte[] payload, InetAddress ip, int port) {
    EndpointConnectionKey key = new EndpointConnectionKey(new InetSocketAddress(ip, port), connectionId);
    long now = System.currentTimeMillis();
    SenderState senderState = sendSequences.computeIfAbsent(key, ignored -> new SenderState());
    int seq = senderState.nextSequence(now);
    
    Dpch packet = Dpch.data(connectionId, seq, payload);
    stubbornLink.sendTracked(packet, ip, port);
    cleanStaleStatesIfNeeded();
  }

  public InboundMessage receive() throws InterruptedException {
    return deliveryQueue.take();
  }

  private void runReceiveLoop() {
    while (running.get()) {
      try {
        // Blocking call that waits for the next message from the stubborn link.
        processInbound(stubbornLink.receive()); 
      } catch (Exception exception) {
        if (!running.get()) {
          break;
        }

        System.err.println("PerfectLink worker error: " + exception.getMessage());
      }
    }
  }

  // Process incoming messages
  private void processInbound(InboundMessage inbound) throws IOException {
    Dpch packet = inbound.packet();
    InetSocketAddress remote = new InetSocketAddress(inbound.senderIp(), inbound.senderPort());
    EndpointConnectionKey key = new EndpointConnectionKey(remote, packet.connectionId());
    long now = System.currentTimeMillis();

    // If an ack is received, cancel the corresponding tracked DATA message in the stubborn link.
    if (packet.type() == DpchType.ACK) {
      SenderState senderState = sendSequences.get(key);
      if (senderState != null) {
        senderState.touch(now);
      }

      TrackedMessage.Key trackedDataKey = new TrackedMessage.Key(packet.connectionId(), packet.sequenceNumber(), DpchType.DATA);
      stubbornLink.cancelTracked(trackedDataKey, remote.getAddress(), remote.getPort());
      cleanStaleStatesIfNeeded();
      return;
    }

    // If a data packet is received, send an ack and manage in-order delivery with flow control.
    if (packet.type() == DpchType.DATA) {
      stubbornLink.sendOnce(Dpch.ack(packet.connectionId(), packet.sequenceNumber(), new byte[0]), remote.getAddress(), remote.getPort());

      // Get or create the receiver state for this stream and process the incoming packet.
      ReceiverState state = receiverStates.computeIfAbsent(key, ignored -> new ReceiverState());
      synchronized (state) {
        state.touch(now);
        int seq = packet.sequenceNumber();

        if (state.isAlreadyDelivered(seq)) {
          return;
        }

        if (state.isOutsideWindow(seq, maxWindowSize)) {
          return;
        }

        // Add packet only if not already buffered.
        if (!state.bufferIfNew(seq, inbound)) {
          return;
        }

        // Deliver in-order packets in cascade.
        while (state.hasNextInOrderReady()) {
          InboundMessage delivered = state.pollNextInOrder();
          deliveryQueue.offer(delivered);
        }
      }

      cleanStaleStatesIfNeeded();
    }
  }

  // Remove stale sender and receiver states that have been idle for too long.
  private void cleanStaleStatesIfNeeded() {
    if (sendSequences.size() <= maxStreamStates && receiverStates.size() <= maxStreamStates) {
      return;
    }
    
    long now = System.currentTimeMillis();
    sendSequences.entrySet().removeIf(entry -> entry.getValue().isStale(now, streamIdleTtlMs));
    receiverStates.entrySet().removeIf(entry -> entry.getValue().isStale(now, streamIdleTtlMs));
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
