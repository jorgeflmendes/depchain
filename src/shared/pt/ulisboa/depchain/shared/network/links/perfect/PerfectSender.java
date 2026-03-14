package pt.ulisboa.depchain.shared.network.links.perfect;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import pt.ulisboa.depchain.shared.logging.Logger;
import pt.ulisboa.depchain.shared.network.packet.DpchPacket;
import pt.ulisboa.depchain.shared.network.packet.DpchType;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedKey;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class PerfectSender {
  private static final Logger logger = new Logger("PerfectSender");
  private final PerfectContext context;

  PerfectSender(PerfectContext context) {
    this.context = ValidationUtils.requireNonNull(context, "context");
  }

  void send(long connectionId, DpchType packetType, boolean withAck, byte[] payload, InetSocketAddress remoteEndpoint) {
    switch (ValidationUtils.requireNonNull(packetType, "packetType")) {
      case ACK -> throw new IllegalArgumentException("ACK must be sent via sendAck");
      case DATA -> {
        if (withAck) {
          throw new IllegalArgumentException("DATA cannot be combined with ACK");
        }
        sendTracked(connectionId, packetType, false, payload, remoteEndpoint);
      }
      case SYN, FIN -> sendTracked(connectionId, packetType, withAck, payload, remoteEndpoint);
    }
  }

  void sendAck(long connectionId, int acknowledgedSequence, DpchType acknowledgedType, InetSocketAddress remoteEndpoint) {
    sendAckBestEffort(connectionId, acknowledgedSequence, remoteEndpoint, PerfectContext.ackPayloadFor(acknowledgedType));
  }

  boolean awaitNoPendingType(long connectionId, InetSocketAddress remoteEndpoint, DpchType packetType, long timeoutMs) throws InterruptedException {
    return context.awaitNoPendingType(connectionId, remoteEndpoint, packetType, timeoutMs);
  }

  void cancelPendingType(long connectionId, InetSocketAddress remoteEndpoint, DpchType packetType) {
    ConnectionKey key = new ConnectionKey(remoteEndpoint, connectionId);
    List<TrackedKey> cancellations = new ArrayList<>();

    // Drop pending tracked packets of this type for this connection.
    context.connectionStates.computeIfPresent(key, (ignored, connectionState) -> {
      cancellations.addAll(connectionState.senderState().cancelPendingType(connectionId, packetType));
      return connectionState;
    });
    context.cancelTrackedBatch(cancellations, remoteEndpoint);
  }

  void sendAckBestEffort(long connectionId, int sequenceNumber, InetSocketAddress remote, byte[] ackPayload) {
    if (!context.isRunning()) {
      return;
    }

    try {
      context.stubbornLink.send(PerfectContext.serializePacket(DpchPacket.from(connectionId, DpchType.ACK, sequenceNumber, ackPayload)), remote);
    } catch (IOException | RuntimeException exception) {
      if (!context.isRunning()) {
        return;
      }
      logger.debug("Failed to send ACK for connection " + connectionId + " seq=" + sequenceNumber + ": " + exception.getMessage());
    }
  }

  private void sendTracked(long connectionId, DpchType type, boolean withAck, byte[] payload, InetSocketAddress remoteEndpoint) {
    ConnectionKey key = new ConnectionKey(remoteEndpoint, connectionId);
    boolean reusePendingControl = (type == DpchType.SYN || type == DpchType.FIN);
    int[] sequenceNumberHolder = new int[1];
    SenderState[] senderStateHolder = new SenderState[1];

    // Create or reuse the sender state atomically for this stream.
    context.connectionStates.compute(key, (ignored, existingState) -> {
      PerfectConnectionState connectionState = existingState;
      if (connectionState == null) {
        connectionState = new PerfectConnectionState();
      }

      SenderState senderState = connectionState.senderState();
      senderStateHolder[0] = senderState;
      sequenceNumberHolder[0] = senderState.nextOrPendingSequence(type, reusePendingControl);
      return connectionState;
    });
    int sequenceNumber = sequenceNumberHolder[0];
    SenderState senderState = senderStateHolder[0];

    DpchPacket packet = DpchPacket.from(connectionId, type, withAck, sequenceNumber, payload);
    context.stubbornLink.sendTrackedWithTerminalNotification(
        new TrackedKey(connectionId, sequenceNumber, Byte.toUnsignedInt(type.code())),
        PerfectContext.serializePacket(packet),
        remoteEndpoint,
        senderState::notifyWaiters);
  }
}

