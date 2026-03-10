package pt.ulisboa.depchain.shared.network.links.perfect;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.dpch.DpchType;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedKey;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class PerfectSender {
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

  boolean waitUntilNoPendingData(long connectionId, InetSocketAddress remoteEndpoint, long timeoutMs) throws InterruptedException {
    return waitUntilNoPendingType(connectionId, remoteEndpoint, DpchType.DATA, timeoutMs);
  }

  boolean waitUntilNoPendingType(long connectionId, InetSocketAddress remoteEndpoint, DpchType packetType, long timeoutMs) throws InterruptedException {
    return context.waitUntilNoPendingType(connectionId, remoteEndpoint, packetType, timeoutMs);
  }

  void cancelPendingType(long connectionId, InetSocketAddress remoteEndpoint, DpchType packetType) {
    ConnectionKey key = new ConnectionKey(remoteEndpoint, connectionId);
    List<TrackedKey> cancellations = new ArrayList<>();

    // Drop pending tracked packets of this type for this connection.
    context.sendSequences.computeIfPresent(key, (ignored, senderState) -> {
      cancellations.addAll(senderState.cancelPendingType(connectionId, packetType));
      return senderState;
    });

    for (TrackedKey trackedKey : cancellations) {
      context.stubbornLink.cancelTracked(trackedKey, remoteEndpoint);
    }
  }

  void sendAckBestEffort(long connectionId, int sequenceNumber, InetSocketAddress remote, byte[] ackPayload) {
    try {
      context.stubbornLink.send(PerfectContext.serializePacket(Dpch.from(connectionId, DpchType.ACK, sequenceNumber, ackPayload)), remote);
    } catch (IOException exception) {
      throw new IllegalStateException("Failed to send ACK for connection " + connectionId + " seq=" + sequenceNumber, exception);
    }
  }

  private void sendTracked(long connectionId, DpchType type, boolean withAck, byte[] payload, InetSocketAddress remoteEndpoint) {
    ConnectionKey key = new ConnectionKey(remoteEndpoint, connectionId);
    boolean reusePendingControl = (type == DpchType.SYN || type == DpchType.FIN);
    int[] sequenceNumberHolder = new int[1];

    // Create or reuse the sender state atomically for this stream.
    context.sendSequences.compute(key, (ignored, existingState) -> {
      SenderState senderState = existingState;
      if (senderState == null) {
        senderState = new SenderState();
      }

      sequenceNumberHolder[0] = senderState.nextOrPendingSequence(type, reusePendingControl);
      return senderState;
    });
    int sequenceNumber = sequenceNumberHolder[0];

    Dpch packet = Dpch.from(connectionId, type, withAck, sequenceNumber, payload);
    context.stubbornLink.sendTracked(new TrackedKey(connectionId, sequenceNumber, Byte.toUnsignedInt(type.code())), PerfectContext.serializePacket(packet), remoteEndpoint);
  }
}
