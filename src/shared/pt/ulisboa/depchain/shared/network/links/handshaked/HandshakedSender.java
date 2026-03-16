package pt.ulisboa.depchain.shared.network.links.handshaked;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.net.InetSocketAddress;

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
    ConnectionState connectionState = context.connectionStateRegistry.getOrCreate(connectionKey);

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
    ConnectionState connectionState = context.connectionStateRegistry.get(connectionKey);

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

    awaitPendingDataClearance(connectionId, remote, "Interrupted while waiting for pending DATA");

    synchronized (connectionState) {
      if (connectionState.shouldSendFin()) {
        context.perfectLink.sendFin(connectionId, remote);
        connectionState.markLocalFinished();
      }
    }

    awaitPendingFinClearance(connectionId, remote, "Interrupted while waiting for pending FIN");

    waitUntilCloseConverged(connectionState);
    context.perfectLink.releaseConnection(connectionId, remote);
  }

  private void closeStatelessConnection(long connectionId, InetSocketAddress remote) {
    try {
      context.perfectLink.sendFin(connectionId, remote);
      awaitPendingFinClearance(connectionId, remote, "Interrupted while waiting for stateless FIN");
    } finally {
      context.perfectLink.releaseConnection(connectionId, remote);
    }
  }

  private void waitUntilFullyEstablished(ConnectionState connectionState, long connectionId, InetSocketAddress remote, boolean shouldWaitForSyn) {
    if (shouldWaitForSyn || hasPendingHandshake(connectionState)) {
      awaitPendingSynClearance(connectionId, remote, "Interrupted while waiting for handshake SYN");
    }

    waitOnConnectionState(connectionState, connectionState::isFullyEstablished, "Connection closed during handshake", "Interrupted while waiting for handshake");
  }

  private void waitUntilCloseConverged(ConnectionState connectionState) {
    waitOnConnectionState(connectionState, connectionState::isCloseConverged, null, "Interrupted while waiting for close convergence");
    ensureRunning();
  }

  private void awaitPendingDataClearance(long connectionId, InetSocketAddress remote, String interruptedMessage) {
    awaitPendingClearance(() -> context.perfectLink.awaitNoPendingData(connectionId, remote, Long.MAX_VALUE), interruptedMessage);
  }

  private void awaitPendingSynClearance(long connectionId, InetSocketAddress remote, String interruptedMessage) {
    awaitPendingClearance(() -> context.perfectLink.awaitNoPendingSyn(connectionId, remote, Long.MAX_VALUE), interruptedMessage);
  }

  private void awaitPendingFinClearance(long connectionId, InetSocketAddress remote, String interruptedMessage) {
    awaitPendingClearance(() -> context.perfectLink.awaitNoPendingFin(connectionId, remote, Long.MAX_VALUE), interruptedMessage);
  }

  private void awaitPendingClearance(InterruptiblePendingWait waitOperation, String interruptedMessage) {
    try {
      if (!waitOperation.await()) {
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

  private boolean hasPendingHandshake(ConnectionState connectionState) {
    synchronized (connectionState) {
      return !connectionState.isFullyEstablished() && !connectionState.isClosing();
    }
  }

  private void waitOnConnectionState(ConnectionState connectionState, java.util.function.BooleanSupplier done, String closingMessage, String interruptedMessage) {
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

  @FunctionalInterface
  private interface InterruptiblePendingWait {
    boolean await() throws InterruptedException;
  }
}
