package pt.ulisboa.depchain.shared.network.links.perfect;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;

import pt.ulisboa.depchain.proto.DpchPacket;
import pt.ulisboa.depchain.proto.DpchPacketType;
import pt.ulisboa.depchain.shared.logging.Logger;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedKey;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class PerfectSender {
  private static final Logger logger = new Logger("PerfectSender");
  private final PerfectContext context;

  PerfectSender(PerfectContext context) {
    this.context = ValidationUtils.requireNonNull(context, "context");
  }

  void send(long connectionId, DpchPacketType packetType, boolean withAck, byte[] payload, InetSocketAddress remoteEndpoint) {
    switch (ValidationUtils.requireNonNull(packetType, "packetType")) {
      case DPCH_PACKET_TYPE_ACK -> throw new IllegalArgumentException("ACK must be sent via sendAck");
      case DPCH_PACKET_TYPE_DATA -> {
        if (withAck) {
          throw new IllegalArgumentException("DATA cannot be combined with ACK");
        }
        sendTracked(connectionId, packetType, false, payload, remoteEndpoint);
      }
      case DPCH_PACKET_TYPE_SYN, DPCH_PACKET_TYPE_FIN -> sendTracked(connectionId, packetType, withAck, payload, remoteEndpoint);
      case DPCH_PACKET_TYPE_UNSPECIFIED, UNRECOGNIZED -> throw new IllegalArgumentException("DPCH packet type must be specified");
    }
  }

  void sendAck(long connectionId, int acknowledgedSequence, DpchPacketType acknowledgedType, InetSocketAddress remoteEndpoint) {
    sendAckBestEffort(connectionId, acknowledgedSequence, remoteEndpoint, PerfectContext.ackPayloadFor(acknowledgedType));
  }

  boolean awaitNoPendingType(long connectionId, InetSocketAddress remoteEndpoint, DpchPacketType packetType, long timeoutMs) throws InterruptedException {
    return context.awaitNoPendingType(connectionId, remoteEndpoint, packetType, timeoutMs);
  }

  void cancelPendingType(long connectionId, InetSocketAddress remoteEndpoint, DpchPacketType packetType) {
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
      DpchPacket packet = DpchPacket.newBuilder().setConnectionId(connectionId).setPacketType(DpchPacketType.DPCH_PACKET_TYPE_ACK).setHasAck(false).setSequenceNumber(sequenceNumber)
          .setPayload(ByteString.copyFrom(ackPayload)).build();
      context.stubbornLink.send(PerfectContext.serializePacket(packet), remote);
    } catch (IOException | RuntimeException exception) {
      if (!context.isRunning()) {
        return;
      }
      logger.debug("Failed to send ACK for connection " + connectionId + " seq=" + sequenceNumber + ": " + exception.getMessage());
    }
  }

  private void sendTracked(long connectionId, DpchPacketType type, boolean withAck, byte[] payload, InetSocketAddress remoteEndpoint) {
    ConnectionKey key = new ConnectionKey(remoteEndpoint, connectionId);
    boolean reusePendingControl = (type == DpchPacketType.DPCH_PACKET_TYPE_SYN || type == DpchPacketType.DPCH_PACKET_TYPE_FIN);
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

    DpchPacket packet = DpchPacket.newBuilder().setConnectionId(connectionId).setPacketType(type).setHasAck(withAck).setSequenceNumber(sequenceNumber)
        .setPayload(ByteString.copyFrom(payload)).build();
    context.stubbornLink.sendTrackedWithTerminalNotification(
        new TrackedKey(connectionId, sequenceNumber, type.getNumber()),
        PerfectContext.serializePacket(packet),
        remoteEndpoint,
        senderState::notifyWaiters);
  }
}

