package pt.ulisboa.depchain.shared.network.links.handshaked.coordinator;

import java.net.InetAddress;

import pt.ulisboa.depchain.shared.network.dpch.DpchType;
import pt.ulisboa.depchain.shared.network.links.handshaked.ConnectionState;
import pt.ulisboa.depchain.shared.network.links.perfect.PerfectLink;

public final class StartHandshakeCoordinator {
  private static final byte[] EMPTY_CONTROL_PAYLOAD = new byte[0];

  private final PerfectLink perfectLink;
  private final long connectionIdleTtlMs;

  public StartHandshakeCoordinator(PerfectLink perfectLink, long connectionIdleTtlMs) {
    this.perfectLink = perfectLink;
    this.connectionIdleTtlMs = connectionIdleTtlMs;
  }

  public void sendReliable(
      ConnectionState connectionState,
      long connectionId,
      byte[] payload,
      InetAddress remoteIp,
      int remotePort) {
    synchronized (connectionState) {
      connectionState.touch(System.currentTimeMillis());
      if (connectionState.isClosing()) {
        throw new IllegalStateException("Connection is closing or closed");
      }

      if (connectionState.shouldSendSyn()) {
        sendControlPacket(connectionId, DpchType.SYN, remoteIp, remotePort);
        connectionState.markLocalEstablished();
        connectionState.notifyAll();
      }
    }

    waitUntilFullyEstablished(connectionState);

    synchronized (connectionState) {
      connectionState.touch(System.currentTimeMillis());
      if (!connectionState.canExchangeData()) {
        throw new IllegalStateException("Connection is closing or closed");
      }
      perfectLink.send(connectionId, DpchType.DATA, false, payload, remoteIp, remotePort);
    }
  }

  // Wait until the connection is fully established (handshake completed).
  private void waitUntilFullyEstablished(ConnectionState connectionState) {
    long deadlineMs = System.currentTimeMillis() + connectionIdleTtlMs;
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

  private static long remainingMsUntil(long deadlineMs) {
    return Math.max(0L, deadlineMs - System.currentTimeMillis());
  }

  private void sendControlPacket(
      long connectionId, DpchType type, InetAddress remoteIp, int remotePort) {
    perfectLink.send(connectionId, type, false, EMPTY_CONTROL_PAYLOAD, remoteIp, remotePort);
  }
}
