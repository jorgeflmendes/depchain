package pt.ulisboa.depchain.shared.network.links.handshaked;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.net.InetSocketAddress;

import pt.ulisboa.depchain.shared.network.dpch.DpchType;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.utils.TimeUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class HandshakedSender {
  private static final byte[] EMPTY_CONTROL_PAYLOAD = new byte[0];

  private final HandshakedContext context;
  private final long connectionIdleTtlMs;

  HandshakedSender(HandshakedContext context, long connectionIdleTtlMs) {
    this.context = ValidationUtils.requireNonNull(context, "context");
    this.connectionIdleTtlMs = ValidationUtils.requirePositiveLong(connectionIdleTtlMs, "connectionIdleTtlMs");
  }

  void send(long connectionId, byte[] payload, InetSocketAddress remote) {
    ValidationUtils.requireAllNonNull(named("payload", payload), named("remote", remote));
    ConnectionKey connectionKey = new ConnectionKey(remote, connectionId);
    ConnectionState connectionState = context.connectionStateRegistry.getOrCreate(connectionKey);

    // Mark the state as active so cleanup cannot remove it mid-send.
    markConnectionStateInUse(connectionState);
    try {
      startHandshakeAndSend(connectionState, connectionId, payload, remote);
      context.connectionStateRegistry.cleanup(TimeUtil.nowMs(), context.closedConnectionsRegistry);
    } finally {
      unmarkConnectionStateInUse(connectionState);
    }
  }

  void sendControlReply(HandshakeReply reply, long connectionId, int sequenceNumber, DpchType inboundType, InetSocketAddress remote) {
    switch (reply) {
      case NONE -> {
      }
      case ACK -> context.perfectLink.sendAck(connectionId, sequenceNumber, inboundType, remote);
      case SYN_ACK -> context.perfectLink.send(connectionId, DpchType.SYN, true, EMPTY_CONTROL_PAYLOAD, remote);
      case FIN_ACK -> context.perfectLink.send(connectionId, DpchType.FIN, true, EMPTY_CONTROL_PAYLOAD, remote);
    }
  }

  private void startHandshakeAndSend(ConnectionState connectionState, long connectionId, byte[] payload, InetSocketAddress remote) {
    synchronized (connectionState) {
      connectionState.touch(TimeUtil.nowMs());
      if (connectionState.isClosing()) {
        throw new IllegalStateException("Connection is closing or closed");
      }

      if (connectionState.shouldSendSyn()) {
        sendControlPacket(connectionId, DpchType.SYN, remote);
        connectionState.markLocalEstablished();
        connectionState.notifyAll();
      }
    }

    waitUntilFullyEstablished(connectionState);

    synchronized (connectionState) {
      connectionState.touch(TimeUtil.nowMs());
      if (!connectionState.canExchangeData()) {
        throw new IllegalStateException("Connection is closing or closed");
      }

      context.perfectLink.send(connectionId, DpchType.DATA, false, payload, remote);
    }
  }

  void closeConnection(long connectionId, InetSocketAddress remote) {
    ValidationUtils.requireNonNull(remote, "remote");
    ConnectionKey connectionKey = new ConnectionKey(remote, connectionId);
    ConnectionState connectionState = context.connectionStateRegistry.get(connectionKey);

    // If there's no state
    if (connectionState == null) {
      closeConnectionWithoutState(connectionKey, connectionId, remote);
      return;
    }

    // Mark the state as active so cleanup cannot remove it mid-close.
    markConnectionStateInUse(connectionState);
    try {
      closeActiveConnection(connectionKey, connectionState, connectionId, remote);
    } finally {
      unmarkConnectionStateInUse(connectionState);
    }
  }

  private void closeActiveConnection(ConnectionKey connectionKey, ConnectionState connectionState, long connectionId, InetSocketAddress remote) {
    long now = TimeUtil.nowMs();
    long deadlineMs = TimeUtil.deadlineAfter(now, closeHandshakeTimeoutMs());
    boolean interrupted = false;

    synchronized (connectionState) {
      connectionState.touch(now);
      connectionState.requestLocalClose();
      connectionState.notifyAll();

      if (connectionState.isCloseConverged()) {
        context.connectionStateRegistry.removeIfSame(connectionKey, connectionState);
        context.closedConnectionsRegistry.markClosed(connectionKey, now);
        return;
      }
    }

    // Wait until there is no pending DATA before sending FIN
    try {
      context.perfectLink.waitUntilNoPendingData(connectionId, remote, remainingMsUntil(deadlineMs));
    } catch (InterruptedException interruptedException) {
      Thread.currentThread().interrupt();
      interrupted = true;
    }

    synchronized (connectionState) {
      connectionState.touch(TimeUtil.nowMs());
      if (connectionState.shouldSendFin()) {
        sendControlPacket(connectionId, DpchType.FIN, remote);
        connectionState.markLocalFinished();
        connectionState.notifyAll();
      }
    }

    interrupted |= waitForCloseConvergedInterruptibly(connectionState, deadlineMs);

    try {
      long remainingMs = remainingMsUntil(deadlineMs);
      if (remainingMs > 0L) {
        context.perfectLink.waitUntilNoPendingType(connectionId, remote, DpchType.FIN, remainingMs);
      }
    } catch (InterruptedException interruptedException) {
      Thread.currentThread().interrupt();
      interrupted = true;
    }

    long graceDeadlineMs = TimeUtil.deadlineAfter(Math.min(finAckGraceTimeoutMs(), TimeUtil.remainingMsUntil(deadlineMs)));
    interrupted |= waitForCloseConvergedInterruptibly(connectionState, graceDeadlineMs);

    finalizeCloseState(connectionKey, connectionState, deadlineMs);
    cleanupConnectionStates();

    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private void closeConnectionWithoutState(ConnectionKey connectionKey, long connectionId, InetSocketAddress remote) {
    long now = TimeUtil.nowMs();
    if (context.closedConnectionsRegistry.isClosedRecently(connectionKey, now)) {
      return;
    }

    sendControlPacket(connectionId, DpchType.FIN, remote);
    context.closedConnectionsRegistry.markClosed(connectionKey, now);
    cleanupConnectionStates();
  }

  private void finalizeCloseState(ConnectionKey connectionKey, ConnectionState connectionState, long closeDeadlineMs) {
    long now = TimeUtil.nowMs();
    synchronized (connectionState) {
      connectionState.touch(now);

      if (connectionState.isLocalFinished() && (connectionState.isCloseConverged() || TimeUtil.hasReachedDeadline(now, closeDeadlineMs))) {
        context.connectionStateRegistry.removeIfSame(connectionKey, connectionState);
        context.closedConnectionsRegistry.markClosed(connectionKey, now);
      }
    }
  }

  private void waitUntilFullyEstablished(ConnectionState connectionState) {
    long deadlineMs = TimeUtil.deadlineAfter(connectionIdleTtlMs);
    synchronized (connectionState) {
      while (!connectionState.isFullyEstablished()) {
        if (connectionState.isClosing()) {
          throw new IllegalStateException("Connection closed during handshake");
        }

        long remainingMs = remainingMsUntil(deadlineMs);
        if (remainingMs <= 0L) {
          throw new IllegalStateException("Connection handshake timed out");
        }

        try {
          connectionState.wait(remainingMs);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("Interrupted while waiting for handshake", interrupted);
        }
      }
    }
  }

  private void cleanupConnectionStates() {
    context.connectionStateRegistry.cleanup(TimeUtil.nowMs(), context.closedConnectionsRegistry);
  }

  private long closeHandshakeTimeoutMs() {
    return Math.min(connectionIdleTtlMs, context.perfectLink.trackedNoPendingTimeoutMs());
  }

  private long finAckGraceTimeoutMs() {
    return Math.max(1L, closeHandshakeTimeoutMs() / 10L);
  }

  private static boolean waitForCloseConvergedInterruptibly(ConnectionState connectionState, long deadlineMs) {
    try {
      synchronized (connectionState) {
        while (!connectionState.isCloseConverged()) {
          long remainingMs = remainingMsUntil(deadlineMs);
          if (remainingMs <= 0L) {
            break;
          }
          connectionState.wait(remainingMs);
        }
      }
      return false;
    } catch (InterruptedException interruptedException) {
      Thread.currentThread().interrupt();
      return true;
    }
  }

  private static long remainingMsUntil(long deadlineMs) {
    return TimeUtil.remainingMsUntil(deadlineMs);
  }

  private void sendControlPacket(long connectionId, DpchType type, InetSocketAddress remote) {
    context.perfectLink.send(connectionId, type, false, EMPTY_CONTROL_PAYLOAD, remote);
  }

  private static void markConnectionStateInUse(ConnectionState connectionState) {
    synchronized (connectionState) {
      connectionState.markInUse();
    }
  }

  private static void unmarkConnectionStateInUse(ConnectionState connectionState) {
    synchronized (connectionState) {
      connectionState.unmarkInUse();
      connectionState.notifyAll();
    }
  }
}
