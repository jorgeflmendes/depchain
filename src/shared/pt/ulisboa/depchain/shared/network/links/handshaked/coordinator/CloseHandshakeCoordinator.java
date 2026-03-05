package pt.ulisboa.depchain.shared.network.links.handshaked.coordinator;

import java.net.InetAddress;

import pt.ulisboa.depchain.shared.network.dpch.DpchType;
import pt.ulisboa.depchain.shared.network.links.handshaked.ConnectionState;
import pt.ulisboa.depchain.shared.network.links.perfect.PerfectLink;
import pt.ulisboa.depchain.shared.network.links.handshaked.registry.ClosedConnectionsRegistry;
import pt.ulisboa.depchain.shared.network.links.handshaked.registry.ConnectionStateRegistry;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;

public final class CloseHandshakeCoordinator {
  private static final byte[] EMPTY_CONTROL_PAYLOAD = new byte[0];

  private final PerfectLink perfectLink;

  private final long connectionIdleTtlMs;

  private final ConnectionStateRegistry connectionStateRegistry;
  private final ClosedConnectionsRegistry closedConnectionsRegistry;

  public CloseHandshakeCoordinator(PerfectLink perfectLink, ConnectionStateRegistry connectionStateRegistry, ClosedConnectionsRegistry closedConnectionsRegistry, long connectionIdleTtlMs) {
    this.perfectLink = perfectLink;
    this.connectionStateRegistry = connectionStateRegistry;
    this.closedConnectionsRegistry = closedConnectionsRegistry;
    this.connectionIdleTtlMs = connectionIdleTtlMs;
  }

  public void closeActiveConnection(ConnectionKey connectionKey, ConnectionState connectionState, long connectionId, InetAddress remoteIp, int remotePort) {
    long now = System.currentTimeMillis();
    long deadlineMs = now + closeHandshakeTimeoutMs();
    boolean interrupted = false;

    synchronized (connectionState) {
      connectionState.touch(now);
      connectionState.requestLocalClose();
      connectionState.notifyAll();
      if (connectionState.isCloseConverged()) {
        connectionStateRegistry.removeIfSame(connectionKey, connectionState);
        closedConnectionsRegistry.markClosed(connectionKey, now);
        return;
      }
    }

    // Wait until no outbound DATA remains before starting close.
    try {
      perfectLink.waitUntilDataDrained(connectionId, remoteIp, remotePort, remainingMsUntil(deadlineMs));
    } catch (InterruptedException interruptedException) {
      Thread.currentThread().interrupt();
      interrupted = true;
    }

    // Send FIN once when local close is requested and local side is not finished yet.
    synchronized (connectionState) {
      connectionState.touch(System.currentTimeMillis());
      if (connectionState.shouldSendFin()) {
        sendControlPacket(connectionId, DpchType.FIN, remoteIp, remotePort);
        connectionState.markLocalFinished();
        connectionState.notifyAll();
      }
    }

    // Wait for close convergence and FIN retry drain.
    interrupted |= waitForCloseConvergedInterruptibly(connectionState, deadlineMs);
    try {
      long remainingMs = remainingMsUntil(deadlineMs);
      if (remainingMs > 0L) {
        perfectLink.waitUntilTypeDrained(connectionId, remoteIp, remotePort, DpchType.FIN, remainingMs);
      }
    } catch (InterruptedException interruptedException) {
      Thread.currentThread().interrupt();
      interrupted = true;
    }

    // Wait a bit more for any final ACKs to arrive before cleaning up the state.
    long graceDeadlineMs = System.currentTimeMillis() + Math.min(finAckGraceTimeoutMs(), remainingMsUntil(deadlineMs));
    interrupted |= waitForCloseConvergedInterruptibly(connectionState, graceDeadlineMs);

    finalizeCloseState(connectionKey, connectionState, deadlineMs);
    cleanup();
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  public void closeConnectionWithoutState(ConnectionKey connectionKey, long connectionId, InetAddress remoteIp, int remotePort) {
    long now = System.currentTimeMillis();
    if (closedConnectionsRegistry.isClosedRecently(connectionKey, now)) {
      return;
    }
    sendControlPacket(connectionId, DpchType.FIN, remoteIp, remotePort);
    closedConnectionsRegistry.markClosed(connectionKey, now);
    cleanup();
  }

  private void finalizeCloseState(
      ConnectionKey connectionKey, ConnectionState connectionState, long closeDeadlineMs) {
    long now = System.currentTimeMillis();
    synchronized (connectionState) {
      connectionState.touch(now);
      if (connectionState.isLocalFinished()
          && (connectionState.isCloseConverged() || now >= closeDeadlineMs)) {
        connectionStateRegistry.removeIfSame(connectionKey, connectionState);
        closedConnectionsRegistry.markClosed(connectionKey, now);
      }
    }
  }

  // Wait until the close handshake converges (both sides finished), or until the deadline expires.
  private static boolean waitForCloseConvergedInterruptibly(
      ConnectionState connectionState, long deadlineMs) {
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

  private void cleanup() {
    connectionStateRegistry.cleanup(System.currentTimeMillis(), closedConnectionsRegistry);
  }

  private long closeHandshakeTimeoutMs() {
    return Math.min(connectionIdleTtlMs, perfectLink.trackedDrainTimeoutMs());
  }

  private long finAckGraceTimeoutMs() {
    return Math.max(1L, closeHandshakeTimeoutMs() / 10L);
  }

  private static long remainingMsUntil(long deadlineMs) {
    return Math.max(0L, deadlineMs - System.currentTimeMillis());
  }

  private void sendControlPacket(long connectionId, DpchType type, InetAddress remoteIp, int remotePort) {
    perfectLink.send(connectionId, type, false, EMPTY_CONTROL_PAYLOAD, remoteIp, remotePort);
  }
}
