package pt.ulisboa.depchain.shared.network.links.handshaked;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.net.InetSocketAddress;
import java.util.function.BooleanSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.ulisboa.depchain.proto.DpchPacketType;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class HandshakedSender {
  private static final Logger logger = LoggerFactory.getLogger(HandshakedSender.class);
  private final HandshakedContext context;

  HandshakedSender(HandshakedContext context) {
    this.context = ValidationUtils.requireNonNull(context, "context");
  }

  void send(long connectionId, byte[] payload, InetSocketAddress remote) {
    ValidationUtils.requireAllNonNull(named("payload", payload), named("remote", remote));
    ConnectionKey connectionKey = new ConnectionKey(remote, connectionId);
    ConnectionState connectionState = context.getOrCreateConnectionState(connectionKey);

    startHandshakeAndSend(connectionState, connectionId, payload, remote);
  }

  void sendHandshakeReply(HandshakeReply reply, long connectionId, int sequenceNumber, DpchPacketType packetType, InetSocketAddress remote) {
    if (!context.isRunning()) {
      return;
    }

    try {
      switch (reply) {
        case NONE -> {
        }
        case ACK -> context.perfectLink.sendAck(connectionId, sequenceNumber, packetType, remote);
        case SYN_ACK -> context.perfectLink.sendSynAck(connectionId, remote);
        case FIN_ACK -> context.perfectLink.sendFinAck(connectionId, remote);
      }
    } catch (RuntimeException exception) {
      if (!context.isRunning()) {
        return;
      }
      logger.debug("Failed to send handshake control reply for connection {}", connectionId, exception);
    }
  }

  private void startHandshakeAndSend(ConnectionState connectionState, long connectionId, byte[] payload, InetSocketAddress remote) {
    boolean shouldWaitForSyn = false;

    synchronized (connectionState) {
      if (connectionState.isClosing()) {
        throw new IllegalStateException("Connection is closing or closed");
      }

      if (connectionState.shouldSendSyn()) {
        context.perfectLink.sendSyn(connectionId, remote);
        connectionState.markLocalEstablished();
        shouldWaitForSyn = true;
      }
    }

    waitUntilFullyEstablished(connectionState, connectionId, remote, shouldWaitForSyn);

    synchronized (connectionState) {
      if (!connectionState.canExchangeData()) {
        throw new IllegalStateException("Connection is closing or closed");
      }

      context.perfectLink.sendData(connectionId, payload, remote);
    }
  }

  void closeConnection(long connectionId, InetSocketAddress remote) {
    ValidationUtils.requireNonNull(remote, "remote");
    ConnectionKey connectionKey = new ConnectionKey(remote, connectionId);
    ConnectionState connectionState = context.getConnectionState(connectionKey);

    // If there's no state
    if (connectionState == null) {
      closeStatelessConnection(connectionId, remote);
      return;
    }

    closeActiveConnection(connectionState, connectionId, remote);
  }

  private void closeActiveConnection(ConnectionState connectionState, long connectionId, InetSocketAddress remote) {
    synchronized (connectionState) {
      connectionState.requestLocalClose();

      if (connectionState.isCloseConverged()) {
        context.perfectLink.releaseConnection(connectionId, remote);
        return;
      }
    }

    awaitPendingClearance(DpchPacketType.DPCH_PACKET_TYPE_DATA, connectionId, remote, "Interrupted while waiting for pending DATA");

    synchronized (connectionState) {
      if (connectionState.shouldSendFin()) {
        context.perfectLink.sendFin(connectionId, remote);
        connectionState.markLocalFinished();
      }
    }

    awaitPendingClearance(DpchPacketType.DPCH_PACKET_TYPE_FIN, connectionId, remote, "Interrupted while waiting for pending FIN");

    waitUntilCloseConverged(connectionState);
    context.perfectLink.releaseConnection(connectionId, remote);
  }

  private void closeStatelessConnection(long connectionId, InetSocketAddress remote) {
    try {
      context.perfectLink.sendFin(connectionId, remote);
      awaitPendingClearance(DpchPacketType.DPCH_PACKET_TYPE_FIN, connectionId, remote, "Interrupted while waiting for stateless FIN");
    } finally {
      context.perfectLink.releaseConnection(connectionId, remote);
    }
  }

  private void waitUntilFullyEstablished(ConnectionState connectionState, long connectionId, InetSocketAddress remote, boolean shouldWaitForSyn) {
    boolean mustWaitForSyn = shouldWaitForSyn;
    synchronized (connectionState) {
      if (!mustWaitForSyn) {
        mustWaitForSyn = !connectionState.isFullyEstablished() && !connectionState.isClosing();
      }
    }
    if (mustWaitForSyn) {
      awaitPendingClearance(DpchPacketType.DPCH_PACKET_TYPE_SYN, connectionId, remote, "Interrupted while waiting for handshake SYN");
    }

    waitOnConnectionState(connectionState, connectionState::isFullyEstablished, "Connection closed during handshake", "Interrupted while waiting for handshake");
  }

  private void waitUntilCloseConverged(ConnectionState connectionState) {
    waitOnConnectionState(connectionState, connectionState::isCloseConverged, null, "Interrupted while waiting for close convergence");
    ensureRunning();
  }

  private void awaitPendingClearance(DpchPacketType packetType, long connectionId, InetSocketAddress remote, String interruptedMessage) {
    try {
      boolean cleared;
      if (packetType == DpchPacketType.DPCH_PACKET_TYPE_SYN) {
        cleared = context.perfectLink.awaitNoPendingSyn(connectionId, remote, Long.MAX_VALUE);
      } else if (packetType == DpchPacketType.DPCH_PACKET_TYPE_FIN) {
        cleared = context.perfectLink.awaitNoPendingFin(connectionId, remote, Long.MAX_VALUE);
      } else {
        cleared = context.perfectLink.awaitNoPendingData(connectionId, remote, Long.MAX_VALUE);
      }

      if (!cleared) {
        ensureRunning();
      }
    } catch (InterruptedException interruptedException) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(interruptedMessage, interruptedException);
    }
  }

  private void ensureRunning() {
    if (!context.isRunning()) {
      throw new IllegalStateException("HandshakedPerfectLink is closed");
    }
  }

  private void waitOnConnectionState(ConnectionState connectionState, BooleanSupplier done, String closingMessage, String interruptedMessage) {
    synchronized (connectionState) {
      while (!done.getAsBoolean()) {
        ensureRunning();
        if (closingMessage != null && connectionState.isClosing()) {
          throw new IllegalStateException(closingMessage);
        }

        try {
          connectionState.wait();
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(interruptedMessage, interrupted);
        }
      }
    }
  }
}
