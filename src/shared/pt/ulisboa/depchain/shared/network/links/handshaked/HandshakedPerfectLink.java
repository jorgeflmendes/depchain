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
  // Avoid allocating a new empty array on every SYN/FIN send.
  private static final byte[] EMPTY_CONTROL_PAYLOAD = new byte[0];

  private final PerfectLink perfectLink;
  private final int maxConnectionStates;
  private final long connectionIdleTtlMs;
  private final Map<EndpointConnectionKey, ConnectionState> connectionStates;
  private final Map<EndpointConnectionKey, Long> closedConnections;
  private final BlockingQueue<InboundMessage> deliveryQueue;
  private final AtomicBoolean running;
  private final Thread workerThread;

  private HandshakedPerfectLink(PerfectLink perfectLink, int maxConnectionStates, long connectionIdleTtlMs) {
    this.perfectLink = Objects.requireNonNull(perfectLink, "perfectLink cannot be null");
    this.maxConnectionStates = ValidationUtils.requirePositiveInt(maxConnectionStates, "maxConnectionStates");
    this.connectionIdleTtlMs = ValidationUtils.requirePositiveLong(connectionIdleTtlMs, "connectionIdleTtlMs");
    this.connectionStates = new ConcurrentHashMap<>();
    this.closedConnections = new ConcurrentHashMap<>();
    this.deliveryQueue = new LinkedBlockingQueue<>();
    this.running = new AtomicBoolean(true);
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

  // Send one DATA message after strict SYN/SYN establishment.
  public void sendReliable(long connectionId, byte[] payload, InetAddress remoteIp, int remotePort) {
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

    // Strict handshake: DATA only after both sides are established.
    waitUntilFullyEstablished(state);

    synchronized (state) {
      state.touch(System.currentTimeMillis());
      ensureNotClosedForSend(state);
      perfectLink.sendReliable(connectionId, payload, remoteIp, remotePort);
    }

    cleanStaleStatesIfNeeded();
  }

  public void closeConnection(long connectionId, InetAddress remoteIp, int remotePort) {
    Objects.requireNonNull(remoteIp, "remoteIp cannot be null");
    ValidationUtils.requireValidPort(remotePort, "remotePort");

    EndpointConnectionKey key = connectionKey(connectionId, remoteIp, remotePort);
    long now = System.currentTimeMillis();
    if (isClosedRecently(key, now)) {
      return;
    }

    ConnectionState state = connectionStates.get(key);
    if (state == null) {
      // Avoid resurrecting a stream with SYN/FIN when no active state exists.
      markClosed(key, now);
      cleanStaleStatesIfNeeded();
      return;
    }

    synchronized (state) {
      state.touch(now);
      if (state.isLocalFinished() || state.isRemoteFinished()) {
        connectionStates.remove(key, state);
        markClosed(key, System.currentTimeMillis());
        return;
      }

      ensureSynSent(state, connectionId, remoteIp, remotePort);
      perfectLink.sendFin(connectionId, EMPTY_CONTROL_PAYLOAD, remoteIp, remotePort);
      state.markLocalFinished();
      state.notifyAll();
    }
    connectionStates.remove(key, state);
    markClosed(key, System.currentTimeMillis());

    cleanStaleStatesIfNeeded();
  }

  public InboundMessage receive() throws InterruptedException {
    return deliveryQueue.take();
  }

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

    long connectionId = inbound.packet().connectionId();
    InetAddress remoteIp = inbound.senderIp();
    int remotePort = inbound.senderPort();
    EndpointConnectionKey key = connectionKey(connectionId, remoteIp, remotePort);
    long now = System.currentTimeMillis();
    if (isClosedRecently(key, now)) {
      return null;
    }

    ConnectionState state = connectionStates.computeIfAbsent(key, ignored -> new ConnectionState());
    boolean deliverData = false;
    boolean shouldRemoveState = false;

    synchronized (state) {
      state.touch(now);

      if (type == DpchType.SYN) {
        state.markRemoteEstablishedIfNotFinished();
        // Passive handshake side: reply with plain SYN (ACK stays separate).
        if (state.shouldSendSyn() && !state.isLocalFinished()) {
          perfectLink.sendSyn(connectionId, EMPTY_CONTROL_PAYLOAD, remoteIp, remotePort);
          state.markLocalEstablished();
        }
        state.notifyAll();
      } else if (type == DpchType.FIN) {
        state.markRemoteFinished();
        state.notifyAll();
        shouldRemoveState = true;
      } else if (state.isFullyEstablished() && !state.isFinished()) {
        deliverData = true;
      }
    }

    if (shouldRemoveState) {
      connectionStates.remove(key, state);
      markClosed(key, System.currentTimeMillis());
    }

    cleanStaleStatesIfNeeded();
    return deliverData ? inbound : null;
  }

  private EndpointConnectionKey connectionKey(long connectionId, InetAddress remoteIp, int remotePort) {
    return new EndpointConnectionKey(new InetSocketAddress(remoteIp, remotePort), connectionId);
  }

  private static void ensureNotClosedForSend(ConnectionState state) {
    if (state.isLocalFinished() || state.isRemoteFinished()) {
      throw new IllegalStateException("Connection already closed by FIN");
    }
  }

  private void ensureSynSent(ConnectionState state, long connectionId, InetAddress remoteIp, int remotePort) {
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

  // Keep memory bounded without dropping active states arbitrarily.
  private void cleanStaleStatesIfNeeded() {
    long now = System.currentTimeMillis();
    pruneClosedConnections(now);

    if (connectionStates.size() > maxConnectionStates) {
      connectionStates.entrySet().removeIf(entry -> entry.getValue().isStale(now, connectionIdleTtlMs));
      if (connectionStates.size() > maxConnectionStates) {
        connectionStates.entrySet().removeIf(entry -> entry.getValue().isFinished());
      }
    }
  }

  private void markClosed(EndpointConnectionKey key, long now) {
    closedConnections.put(key, now);
  }

  private boolean isClosedRecently(EndpointConnectionKey key, long now) {
    Long closedAt = closedConnections.get(key);
    if (closedAt == null) {
      return false;
    }

    if ((now - closedAt) > connectionIdleTtlMs) {
      closedConnections.remove(key, closedAt);
      return false;
    }
    return true;
  }

  private void pruneClosedConnections(long now) {
    if (closedConnections.isEmpty()) {
      return;
    }

    closedConnections.entrySet().removeIf(entry -> (now - entry.getValue()) > connectionIdleTtlMs);
    if (closedConnections.size() <= maxConnectionStates) {
      return;
    }

    closedConnections.entrySet().removeIf(entry -> {
      if (closedConnections.size() <= maxConnectionStates) {
        return false;
      }
      return !connectionStates.containsKey(entry.getKey());
    });
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
