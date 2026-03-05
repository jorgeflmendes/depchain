package pt.ulisboa.depchain.shared.network.links.handshaked;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;
import static pt.ulisboa.depchain.shared.utils.ValidationUtils.requireAllNonNull;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import pt.ulisboa.depchain.shared.network.dpch.DpchType;
import pt.ulisboa.depchain.shared.network.links.handshaked.coordinator.CloseHandshakeCoordinator;
import pt.ulisboa.depchain.shared.network.links.handshaked.coordinator.StartHandshakeCoordinator;
import pt.ulisboa.depchain.shared.network.links.handshaked.registry.ClosedConnectionsRegistry;
import pt.ulisboa.depchain.shared.network.links.handshaked.registry.ConnectionStateRegistry;
import pt.ulisboa.depchain.shared.network.links.perfect.PerfectLink;
import pt.ulisboa.depchain.shared.network.links.handshaked.InboundHandshakeDecider.ControlReply;
import pt.ulisboa.depchain.shared.network.links.handshaked.InboundHandshakeDecider.InboundDecision;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundMessage;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class HandshakedPerfectLink implements AutoCloseable {
  private static final byte[] EMPTY_CONTROL_PAYLOAD = new byte[0];

  private final PerfectLink perfectLink;

  // Connection state tracking maps
  private final ConnectionStateRegistry connectionStateRegistry;
  private final ClosedConnectionsRegistry closedConnectionsRegistry;
  
  // Coordinators for managing handshake processes
  private final StartHandshakeCoordinator startCoordinator;
  private final CloseHandshakeCoordinator closeCoordinator;

  // Queue for delivering inbound messages to the application layer after handshake processing
  private final BlockingQueue<InboundMessage> deliveryQueue = new LinkedBlockingQueue<>();

  // Worker thread and running flag to allow graceful shutdown
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final Thread workerThread;

  private HandshakedPerfectLink(PerfectLink perfectLink, int maxConnectionStates, long connectionIdleTtlMs) {
    this.perfectLink = ValidationUtils.requireNonNull(perfectLink, "perfectLink");
    int checkedMaxConnectionStates = ValidationUtils.requirePositiveInt(maxConnectionStates, "maxConnectionStates");
    long checkedConnectionIdleTtlMs = ValidationUtils.requirePositiveLong(connectionIdleTtlMs, "connectionIdleTtlMs");

    this.connectionStateRegistry = new ConnectionStateRegistry(checkedMaxConnectionStates, checkedConnectionIdleTtlMs); // Active per-connection handshake state.
    this.closedConnectionsRegistry = new ClosedConnectionsRegistry(checkedConnectionIdleTtlMs, checkedMaxConnectionStates);
    this.startCoordinator = new StartHandshakeCoordinator(this.perfectLink, checkedConnectionIdleTtlMs);
    this.closeCoordinator = new CloseHandshakeCoordinator(this.perfectLink, this.connectionStateRegistry, this.closedConnectionsRegistry, checkedConnectionIdleTtlMs);

    this.workerThread = Thread.ofVirtual().name("handshaked-perfect-link").start(this::runReceiveLoop);
  }

  public static HandshakedPerfectLink bind(InetAddress bindAddress, int port, PerfectLink.BuildConfig config) throws IOException {
    PerfectLink perfect = PerfectLink.bind(bindAddress, port, config);
    PerfectLink.Config perfectConfig = config.perfect();
    return new HandshakedPerfectLink(perfect, perfectConfig.maxStreamStates(), perfectConfig.streamIdleTtlMs());
  }

  public static HandshakedPerfectLink unbound(PerfectLink.BuildConfig config) throws IOException {
    PerfectLink perfect = PerfectLink.unbound(config);
    PerfectLink.Config perfectConfig = config.perfect();
    return new HandshakedPerfectLink(perfect, perfectConfig.maxStreamStates(), perfectConfig.streamIdleTtlMs());
  }

  // Sends DATA through a started handshake session (or triggers SYN flow if needed).
  public void sendReliable(long connectionId, byte[] payload, InetAddress remoteIp, int remotePort) {
    requireAllNonNull(named("payload", payload), named("remoteIp", remoteIp));
    ValidationUtils.requireValidPort(remotePort, "remotePort");

    ConnectionKey connectionKey = ConnectionKey.from(remoteIp, remotePort, connectionId);
    ConnectionState connectionState = connectionStateRegistry.getOrCreate(connectionKey); // Reuses stream state across DATA sends.
    startCoordinator.sendReliable(connectionState, connectionId, payload, remoteIp, remotePort);

    connectionStateRegistry.cleanup(System.currentTimeMillis(), closedConnectionsRegistry); // Opportunistic bounded-state cleanup.
  }

  public InboundMessage receive() throws InterruptedException {
    return deliveryQueue.take();
  }

  public InboundMessage receive(long timeoutMs) throws InterruptedException {
    return deliveryQueue.poll(ValidationUtils.requireNonNegativeLong(timeoutMs, "timeoutMs"), TimeUnit.MILLISECONDS);
  }

  // Receives from PerfectLink, applies handshake gatekeeping, then forwards deliverable DATA.
  private void runReceiveLoop() {
    while (running.get()) {
      try {
        InboundMessage delivered = handleInbound(perfectLink.receive());
        if (delivered != null) {
          deliveryQueue.offer(delivered);
        }
      } catch (InterruptedException interrupted) {
        if (!running.get()) {
          break;
        }
        Thread.currentThread().interrupt();
        break;
      } catch (RuntimeException exception) {
        if (!running.get()) {
          break;
        }
        System.err.println("HandshakedPerfectLink worker error: " + exception.getMessage());
      }
    }
  }

  // Starts FIN flow using tracked state when available; otherwise closes using stateless fallback.
  public void closeConnection(long connectionId, InetAddress remoteIp, int remotePort) {
    ValidationUtils.requireNonNull(remoteIp, "remoteIp");
    ValidationUtils.requireValidPort(remotePort, "remotePort");

    ConnectionKey connectionKey = ConnectionKey.from(remoteIp, remotePort, connectionId);
    ConnectionState connectionState = connectionStateRegistry.get(connectionKey);
    if (connectionState == null) { // No active local state, still attempt remote close handshake.
      closeCoordinator.closeConnectionWithoutState(connectionKey, connectionId, remoteIp, remotePort);
      return;
    }

    closeCoordinator.closeActiveConnection(connectionKey, connectionState, connectionId, remoteIp, remotePort);
  }

  // Applies handshake state machine for inbound packet and decides whether DATA reaches the app.
  private InboundMessage handleInbound(InboundMessage inbound) {
    DpchType packetType = inbound.packet().type();
    if (!InboundHandshakeDecider.handlesInboundType(packetType)) { // Defensive filter: ignore unsupported types.
      return null;
    }

    long connectionId = inbound.packet().connectionId();
    int sequenceNumber = inbound.packet().sequenceNumber();
    InetAddress remoteIp = inbound.senderIp();
    int remotePort = inbound.senderPort();
    
    ConnectionKey connectionKey = ConnectionKey.from(remoteIp, remotePort, connectionId);
    long now = System.currentTimeMillis();
    if (closedConnectionsRegistry.isClosedRecently(connectionKey, now)) { // Recently closed: only re-ACK control retries.
      if (packetType == DpchType.SYN || packetType == DpchType.FIN) {
        sendControlReply(ControlReply.ACK, connectionId, sequenceNumber, packetType, remoteIp, remotePort);
      }
      return null;
    }

    ConnectionState connectionState = connectionStateRegistry.getOrCreate(connectionKey);
    InboundDecision decision;
    synchronized (connectionState) { // Decision reads/mutates state atomically with sender wait/notify.
      decision = InboundHandshakeDecider.decideInboundLocked(connectionState, packetType, now);
    }

    sendControlReply(decision.reply(), connectionId, sequenceNumber, packetType, remoteIp, remotePort);
    if (packetType == DpchType.SYN || packetType == DpchType.FIN) { // Wake threads waiting for handshake progression.
      synchronized (connectionState) {
        connectionState.notifyAll();
      }
    }
    connectionStateRegistry.cleanup(System.currentTimeMillis(), closedConnectionsRegistry);

    if (decision.deliverData()) { // Only data packets are delivered to the application layer
      return inbound;
    }
    return null;
  }

  // Maps handshake decision reply to concrete control packet emission on PerfectLink.
  private void sendControlReply(ControlReply reply, long connectionId, int sequenceNumber, DpchType inboundType, InetAddress remoteIp, int remotePort) {
    switch (reply) {
      case NONE -> {}
      case ACK -> perfectLink.sendAck(connectionId, sequenceNumber, inboundType, remoteIp, remotePort);
      case SYN_ACK -> perfectLink.send(connectionId, DpchType.SYN, true, EMPTY_CONTROL_PAYLOAD, remoteIp, remotePort);
      case FIN_ACK -> perfectLink.send(connectionId, DpchType.FIN, true, EMPTY_CONTROL_PAYLOAD, remoteIp, remotePort);
    }
  }

  @Override
  public void close() throws Exception {
    if (!running.compareAndSet(true, false)) {
      return;
    }
    try {
      perfectLink.close();
    } finally {
      workerThread.interrupt();
      try {
        workerThread.join(2_000L);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
