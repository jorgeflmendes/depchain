package pt.ulisboa.depchain.shared.network.links.handshaked;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.net.InetSocketAddress;

import pt.ulisboa.depchain.shared.network.dpch.DpchType;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class HandshakedSender {
  private static final byte[] EMPTY_CONTROL_PAYLOAD = new byte[0];

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
      if (connectionState.isClosing()) {
        throw new IllegalStateException("Connection is closing or closed");
      }

      if (connectionState.shouldSendSyn()) {
        sendControlPacket(connectionId, DpchType.SYN, remote);
        connectionState.markLocalEstablished();
        connectionState.notifyAll();
      }
    }

    waitUntilFullyEstablished(connectionState, connectionId, remote);

    synchronized (connectionState) {
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
      closeConnectionWithoutState(connectionId, remote);
      return;
    }

    closeActiveConnection(connectionKey, connectionState, connectionId, remote);
  }

  private void closeActiveConnection(ConnectionKey connectionKey, ConnectionState connectionState, long connectionId, InetSocketAddress remote) {
    synchronized (connectionState) {
      connectionState.requestLocalClose();
      connectionState.notifyAll();

      if (connectionState.isCloseConverged()) {
        context.connectionStateRegistry.removeIfSame(connectionKey, connectionState);
        return;
      }
    }

    try {
      context.perfectLink.waitUntilNoPendingData(connectionId, remote, Long.MAX_VALUE);
    } catch (InterruptedException interruptedException) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for pending DATA to drain", interruptedException);
    }

    synchronized (connectionState) {
      if (connectionState.shouldSendFin()) {
        sendControlPacket(connectionId, DpchType.FIN, remote);
        connectionState.markLocalFinished();
        connectionState.notifyAll();
      }
    }

    try {
      context.perfectLink.waitUntilNoPendingType(connectionId, remote, DpchType.FIN, Long.MAX_VALUE);
    } catch (InterruptedException interruptedException) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for pending FIN to drain", interruptedException);
    }

    waitUntilCloseConverged(connectionState);
    context.connectionStateRegistry.removeIfSame(connectionKey, connectionState);
  }

  private void closeConnectionWithoutState(long connectionId, InetSocketAddress remote) {
    sendControlPacket(connectionId, DpchType.FIN, remote);
  }

  private void waitUntilFullyEstablished(ConnectionState connectionState, long connectionId, InetSocketAddress remote) {
    synchronized (connectionState) {
      while (!connectionState.isFullyEstablished()) {
        if (connectionState.isClosing()) {
          throw new IllegalStateException("Connection closed during handshake");
        }

        context.perfectLink.throwIfTrackedFailed(connectionId, remote, DpchType.SYN);

        try {
          connectionState.wait(100L);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("Interrupted while waiting for handshake", interrupted);
        }
      }
    }
  }

  private static void waitUntilCloseConverged(ConnectionState connectionState) {
    synchronized (connectionState) {
      while (!connectionState.isCloseConverged()) {
        try {
          connectionState.wait(100L);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("Interrupted while waiting for close convergence", interrupted);
        }
      }
    }
  }

  private void sendControlPacket(long connectionId, DpchType type, InetSocketAddress remote) {
    context.perfectLink.send(connectionId, type, false, EMPTY_CONTROL_PAYLOAD, remote);
  }
}
