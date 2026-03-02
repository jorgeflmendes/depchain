package pt.ulisboa.depchain.shared.network.links.handshaked;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import pt.ulisboa.depchain.shared.network.dpch.DpchType;
import pt.ulisboa.depchain.shared.network.links.perfect.PerfectLink;
import pt.ulisboa.depchain.shared.network.model.EndpointConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundMessage;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class HandshakedPerfectLink implements AutoCloseable {
  // Just to prevent allocation of new empty arrays on every SYN/FIN send.
  private static final byte[] EMPTY_CONTROL_PAYLOAD = new byte[0];

  // Lower-level perfect link used for actual message sending/receiving.
  private final PerfectLink perfectLink;

  // Configuration for connection state management.
  private final int maxConnectionStates;
  private final long connectionIdleTtlMs;
  private final Map<EndpointConnectionKey, ConnectionState> connectionStates;
  private final BlockingQueue<InboundMessage> deliveryQueue;
  private final AtomicBoolean running;
  private final Thread workerThread;

  private HandshakedPerfectLink(PerfectLink perfectLink, int maxConnectionStates, long connectionIdleTtlMs) {
    this.perfectLink = Objects.requireNonNull(perfectLink, "perfectLink cannot be null");
    this.maxConnectionStates = ValidationUtils.requirePositiveInt(maxConnectionStates, "maxConnectionStates");
    this.connectionIdleTtlMs = ValidationUtils.requirePositiveLong(connectionIdleTtlMs, "connectionIdleTtlMs");
    this.connectionStates = new ConcurrentHashMap<>();
    this.deliveryQueue = new LinkedBlockingQueue<>();
    this.running = new AtomicBoolean(true);
    this.workerThread = Thread.ofVirtual().name("handshaked-perfect-link").start(this::runReceiveLoop);
  }

  // Build a HandshakedPerfectLink bound to a local address/port.
  public static HandshakedPerfectLink bind(InetAddress bindAddress, int port, PerfectLink.BuildConfig config) throws IOException {
    PerfectLink perfect = PerfectLink.bind(bindAddress, port, config);
    PerfectLink.Config perfectConfig = config.perfect();
    return new HandshakedPerfectLink(perfect, perfectConfig.maxStreamStates(), perfectConfig.streamIdleTtlMs());
  }

  // Build a HandshakedPerfectLink with an ephemeral local socket.
  public static HandshakedPerfectLink unbound(PerfectLink.BuildConfig config) throws IOException {
    PerfectLink perfect = PerfectLink.unbound(config);
    PerfectLink.Config perfectConfig = config.perfect();
    return new HandshakedPerfectLink(perfect, perfectConfig.maxStreamStates(), perfectConfig.streamIdleTtlMs());
  }

  // Open a connection to the specified remote endpoint, performing the SYN handshake (if not already done for this connection).
  public void openConnection(int connectionId, InetAddress remoteIp, int remotePort) {
    Objects.requireNonNull(remoteIp, "remoteIp cannot be null");
    ValidationUtils.requireValidPort(remotePort, "remotePort");

    EndpointConnectionKey key = connectionKey(connectionId, remoteIp, remotePort);
    ConnectionState state = connectionStates.computeIfAbsent(key, ignored -> new ConnectionState());
    long now = System.currentTimeMillis();

    synchronized (state) {
      state.touch(now);
      ensureNotClosedForSend(state);
      ensureSynSent(state, connectionId, remoteIp, remotePort);
    }
    waitUntilFullyEstablished(state);

    cleanStaleStatesIfNeeded();
  }

  // Send a reliable message on an open connection, performing the SYN handshake (if not already done for this connection).
  public void sendReliable(int connectionId, byte[] payload, InetAddress remoteIp, int remotePort) {
    Objects.requireNonNull(payload, "payload cannot be null");
    Objects.requireNonNull(remoteIp, "remoteIp cannot be null");
    ValidationUtils.requireValidPort(remotePort, "remotePort");

    EndpointConnectionKey key = connectionKey(connectionId, remoteIp, remotePort);
    ConnectionState state = connectionStates.computeIfAbsent(key, ignored -> new ConnectionState());
    long now = System.currentTimeMillis();

    synchronized (state) {
      state.touch(now);
      ensureNotClosedForSend(state);
      ensureSynSent(state, connectionId, remoteIp, remotePort);
    }

    // Strict handshake: do not send DATA until both sides are established.
    waitUntilFullyEstablished(state);

    synchronized (state) {
      state.touch(System.currentTimeMillis());
      ensureNotClosedForSend(state);
      perfectLink.sendReliable(connectionId, payload, remoteIp, remotePort);
    }

    cleanStaleStatesIfNeeded();
  }

  public void closeConnection(int connectionId, InetAddress remoteIp, int remotePort) {
    Objects.requireNonNull(remoteIp, "remoteIp cannot be null");
    ValidationUtils.requireValidPort(remotePort, "remotePort");

    EndpointConnectionKey key = connectionKey(connectionId, remoteIp, remotePort);
    ConnectionState state = connectionStates.computeIfAbsent(key, ignored -> new ConnectionState());
    long now = System.currentTimeMillis();

    synchronized (state) {
      state.touch(now);
      if (state.isLocalFinished()) {
        return;
      }

      ensureSynSent(state, connectionId, remoteIp, remotePort);
      perfectLink.sendFin(connectionId, EMPTY_CONTROL_PAYLOAD, remoteIp, remotePort);
      state.markLocalFinished();
      state.notifyAll();
    }

    cleanStaleStatesIfNeeded();
  }

  // Wait for the next DATA message from a fully established connection.
  public InboundMessage receive() throws InterruptedException {
    return deliveryQueue.take();
  }

  // Wait for the next DATA message from a fully established connection.
  public InboundMessage receive(long timeoutMs) throws InterruptedException {
    long sanitizedTimeoutMs = ValidationUtils.requireNonNegativeLong(timeoutMs, "timeoutMs");
    return deliveryQueue.poll(sanitizedTimeoutMs, TimeUnit.MILLISECONDS);
  }

  private void runReceiveLoop() {
    while (running.get()) {
      try {
        InboundMessage inbound = perfectLink.receive();
        InboundMessage delivered = processInbound(inbound);
        if (delivered != null) {
          deliveryQueue.offer(delivered);
        }
      } catch (InterruptedException interrupted) {
        if (!running.get()) {
          break;
        }
        Thread.currentThread().interrupt();
        break;
      } catch (Exception exception) {
        if (!running.get()) {
          break;
        }
        System.err.println("HandshakedPerfectLink worker error: " + exception.getMessage());
      }
    }
  }

  private InboundMessage processInbound(InboundMessage inbound) {
    DpchType type = inbound.packet().type();
    if (type != DpchType.SYN && type != DpchType.FIN && type != DpchType.DATA) {
      return null;
    }

    int connectionId = inbound.packet().connectionId();
    InetAddress remoteIp = inbound.senderIp();
    int remotePort = inbound.senderPort();
    EndpointConnectionKey key = connectionKey(connectionId, remoteIp, remotePort);
    ConnectionState state = connectionStates.computeIfAbsent(key, ignored -> new ConnectionState());
    boolean deliverData = false;
    long now = System.currentTimeMillis();

    synchronized (state) {
      state.touch(now);

      if (type == DpchType.SYN) {
        state.markRemoteEstablishedIfNotFinished();
        // Passive side of the handshake: reply with SYN once.
        if (state.shouldSendSyn() && !state.isLocalFinished()) {
          perfectLink.sendSyn(connectionId, EMPTY_CONTROL_PAYLOAD, remoteIp, remotePort);
          state.markLocalEstablished();
        }
        state.notifyAll();
      } else if (type == DpchType.FIN) {
        state.markRemoteFinished();
        state.notifyAll();
      } else if (state.isFullyEstablished() && !state.isFinished()) {
        deliverData = true;
      }
    }

    cleanStaleStatesIfNeeded();
    return deliverData ? inbound : null; // DATA before full handshake or after FIN is ignored by design.
  }

  private EndpointConnectionKey connectionKey(int connectionId, InetAddress remoteIp, int remotePort) {
    return new EndpointConnectionKey(new InetSocketAddress(remoteIp, remotePort), connectionId);
  }

  private static void ensureNotClosedForSend(ConnectionState state) {
    if (state.isLocalFinished() || state.isRemoteFinished()) {
      throw new IllegalStateException("Connection already closed by FIN");
    }
  }

  // Ensure that a SYN message is sent for this connection if it hasn't been sent already, to establish the connection before sending any reliable messages.
  private void ensureSynSent(ConnectionState state, int connectionId, InetAddress remoteIp, int remotePort) {
    if (state.shouldSendSyn()) {
      perfectLink.sendSyn(connectionId, EMPTY_CONTROL_PAYLOAD, remoteIp, remotePort);
      state.markLocalEstablished();
      state.notifyAll();
    }
  }

  private void waitUntilFullyEstablished(ConnectionState state) {
    long deadlineMs = System.currentTimeMillis() + connectionIdleTtlMs;
    synchronized (state) {
      while (!state.isFullyEstablished()) {
        if (state.isFinished()) {
          throw new IllegalStateException("Connection closed during handshake");
        }

        long remainingMs = deadlineMs - System.currentTimeMillis();
        if (remainingMs <= 0L) {
          throw new IllegalStateException("Connection handshake timed out");
        }

        try {
          state.wait(remainingMs);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("Interrupted while waiting for handshake", interrupted);
        }
      }
    }
  }

  // Clean up stale connection states that have been idle for too long or already finished, if the total number of tracked states exceeds the configured maximum.
  private void cleanStaleStatesIfNeeded() {
    if (connectionStates.size() <= maxConnectionStates) {
      return;
    }

    long now = System.currentTimeMillis();
    connectionStates.entrySet().removeIf(entry -> entry.getValue().isStale(now, connectionIdleTtlMs));
    if (connectionStates.size() <= maxConnectionStates) {
      return;
    }

    connectionStates.entrySet().removeIf(entry -> entry.getValue().isFinished());
    if (connectionStates.size() <= maxConnectionStates) {
      return;
    }

    for (EndpointConnectionKey key : connectionStates.keySet()) {
      if (connectionStates.size() <= maxConnectionStates) {
        break;
      }
      connectionStates.remove(key);
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
