package pt.ulisboa.depchain.shared.network.links.perfect;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.dpch.DpchType;
import pt.ulisboa.depchain.shared.network.links.LinkFailureException;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedKey;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.utils.TimeUtil;
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

  // TODO: maybe move this to another file
  static final class SenderState {
    private int nextSequence;
    private final NavigableMap<Integer, DpchType> inFlightBySeq = new TreeMap<>();

    synchronized int nextOrPendingSequence(DpchType type, boolean reusePendingSameType) {
      if (reusePendingSameType) {
        for (Map.Entry<Integer, DpchType> entry : inFlightBySeq.entrySet()) {
          if (entry.getValue() == type) {
            return entry.getKey();
          }
        }
      }

      if (nextSequence > Dpch.MAX_PACKET_NUMBER) {
        throw new IllegalStateException("Sender sequence number exhausted for stream");
      }

      int sequence = nextSequence++;
      inFlightBySeq.put(sequence, type);
      notifyAll();
      return sequence;
    }

    synchronized List<TrackedKey> acknowledge(long connectionId, int sequenceNumber, DpchType acknowledgedType) {
      List<TrackedKey> cancellations = new ArrayList<>();

      if (acknowledgedType == DpchType.FIN) {
        Iterator<Map.Entry<Integer, DpchType>> iterator = inFlightBySeq.headMap(sequenceNumber, true).entrySet().iterator();

        while (iterator.hasNext()) {
          Map.Entry<Integer, DpchType> entry = iterator.next();
          cancellations.add(new TrackedKey(connectionId, entry.getKey(), Byte.toUnsignedInt(entry.getValue().code())));
          iterator.remove();
        }

        notifyAll();
        return cancellations;
      }

      DpchType trackedType = inFlightBySeq.get(sequenceNumber);
      if (trackedType == acknowledgedType) {
        inFlightBySeq.remove(sequenceNumber);
        cancellations.add(new TrackedKey(connectionId, sequenceNumber, Byte.toUnsignedInt(acknowledgedType.code())));
        notifyAll();
      }

      return cancellations;
    }

    synchronized List<TrackedKey> cancelPendingType(long connectionId, DpchType packetType) {
      List<TrackedKey> cancellations = new ArrayList<>();
      Iterator<Map.Entry<Integer, DpchType>> iterator = inFlightBySeq.entrySet().iterator();

      while (iterator.hasNext()) {
        Map.Entry<Integer, DpchType> entry = iterator.next();
        if (entry.getValue() != packetType) {
          continue;
        }

        cancellations.add(new TrackedKey(connectionId, entry.getKey(), Byte.toUnsignedInt(packetType.code())));
        iterator.remove();
      }

      notifyAll();
      return cancellations;
    }

    synchronized boolean waitUntilNoPending(PerfectContext context, long connectionId, InetSocketAddress remoteEndpoint, DpchType type, long deadlineMs)
        throws InterruptedException {
      while (hasPending(type)) {
        LinkFailureException failure = failureForTypeOrNull(context, connectionId, remoteEndpoint, type);
        if (failure != null) {
          throw failure;
        }

        long remainingMs = TimeUtil.remainingMsUntil(deadlineMs);
        if (remainingMs <= 0L) {
          return false;
        }
        wait(Math.min(remainingMs, 100L));
      }
      return true;
    }

    private boolean hasPending(DpchType type) {
      for (DpchType inFlightType : inFlightBySeq.values()) {
        if (inFlightType == type) {
          return true;
        }
      }
      return false;
    }

    synchronized LinkFailureException failureForTypeOrNull(PerfectContext context, long connectionId, InetSocketAddress remoteEndpoint, DpchType type) {
      for (Map.Entry<Integer, DpchType> entry : inFlightBySeq.entrySet()) {
        if (entry.getValue() != type) {
          continue;
        }

        LinkFailureException failure = context.trackedFailureOrNull(connectionId, remoteEndpoint, entry.getKey(), entry.getValue());
        if (failure != null) {
          return failure;
        }
      }

      return null;
    }
  }
}
