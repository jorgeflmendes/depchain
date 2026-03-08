package pt.ulisboa.depchain.shared.network.links.perfect;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.dpch.DpchType;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedKey;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.TimeUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class PerfectReceiver {
  private final PerfectContext context;
  private final PerfectSender sender;

  PerfectReceiver(PerfectContext context, PerfectSender sender) {
    ValidationUtils.requireAllNonNull(named("context", context), named("sender", sender));
    this.context = context;
    this.sender = sender;
  }

  void runInboundLoop() {
    while (context.running.get()) {
      try {
        handleInbound(PerfectContext.decodeInboundPacket(context.stubbornLink.receive()));
      } catch (IOException | RuntimeException exception) {
        if (!context.running.get()) {
          break;
        }
        System.err.println("PerfectLink worker error: " + exception.getMessage());
      }
    }
  }

  private void handleInbound(InboundPacket inbound) {
    Dpch packet = inbound.packet();
    InetSocketAddress remote = inbound.sender();
    ConnectionKey key = new ConnectionKey(remote, packet.connectionId());
    long now = TimeUtil.nowMs();

    DpchType reliableType = packet.reliableTypeOrNull();
    if (packet.hasType(DpchType.ACK)) {
      handleAck(packet, reliableType, key, remote, now);
    }
    if (reliableType != null) {
      handleReliable(inbound, packet, reliableType, key, now);
    }
    context.cleanStaleStatesIfNeeded(now);
  }

  private void handleAck(Dpch ackPacket, DpchType fallbackAcknowledgedType, ConnectionKey key, InetSocketAddress remote, long now) {
    DpchType acknowledgedType = PerfectContext.decodeAcknowledgedType(ackPacket.payload(), fallbackAcknowledgedType);
    int acknowledgedSequence = ackPacket.sequenceNumber();
    if (acknowledgedType == null || acknowledgedSequence < 0) {
      return;
    }

    List<TrackedKey> cancellations = new java.util.ArrayList<>();
    // Resolve ACKs against the current sender state without racing with cleanup.
    context.sendSequences.computeIfPresent(key, (ignored, senderState) -> {
      cancellations.addAll(senderState.acknowledge(ackPacket.connectionId(), acknowledgedSequence, acknowledgedType, now));
      return senderState;
    });
    for (TrackedKey trackedKey : cancellations) {
      context.stubbornLink.cancelTracked(trackedKey, remote);
    }
  }

  private void handleReliable(InboundPacket inbound, Dpch packet, DpchType reliableType, ConnectionKey key, long now) {
    List<InboundPacket> readyToDeliver = new java.util.ArrayList<>();
    boolean[] shouldAckData = new boolean[1];

    // Create or reuse the receiver state atomically for this stream.
    context.receiverStates.compute(key, (ignored, existingState) -> {
      ReceiverState receiverState = existingState;
      if (receiverState == null) {
        Integer persisted = context.deliveredSequenceFloors.remove(key);
        int nextExpectedSeq = 0;
        if (persisted != null) {
          nextExpectedSeq = persisted;
        }

        receiverState = new ReceiverState(nextExpectedSeq);
      }

      synchronized (receiverState) {
        receiverState.touch(now);
        int sequenceNumber = packet.sequenceNumber();
        if (sequenceNumber < 0) {
          return receiverState;
        }

        if (receiverState.isAlreadyDelivered(sequenceNumber)) {
          if (reliableType == DpchType.DATA) {
            shouldAckData[0] = true;
          } else {
            readyToDeliver.add(inbound);
          }
        } else if (!receiverState.isOutsideWindow(sequenceNumber, context.maxWindowSize)) {
          if (reliableType == DpchType.DATA) {
            shouldAckData[0] = true;
          }
          if (receiverState.bufferIfNew(sequenceNumber, inbound)) {
            while (receiverState.hasNextInOrderReady()) {
              readyToDeliver.add(receiverState.pollNextInOrder());
            }
          }
        }
      }

      return receiverState;
    });

    for (InboundPacket delivered : readyToDeliver) {
      context.deliveryQueue.offer(delivered);
    }

    if (shouldAckData[0]) {
      sender.sendAckBestEffort(packet.connectionId(), packet.sequenceNumber(), inbound.sender(), PerfectContext.ACK_DATA);
    }
  }

  // TODO: maybe move to another class
  static final class ReceiverState {
    private int nextExpectedSeq;
    private volatile long lastTouchedAtMs = System.currentTimeMillis();
    private final NavigableMap<Integer, InboundPacket> bufferedBySeq = new TreeMap<>();

    ReceiverState(int nextExpectedSeq) {
      this.nextExpectedSeq = Math.max(0, nextExpectedSeq);
    }

    void touch(long now) {
      lastTouchedAtMs = now;
    }

    boolean isAlreadyDelivered(int sequenceNumber) {
      return sequenceNumber < nextExpectedSeq;
    }

    boolean isOutsideWindow(int sequenceNumber, int maxWindowSize) {
      return (long) sequenceNumber >= (long) nextExpectedSeq + maxWindowSize;
    }

    boolean bufferIfNew(int sequenceNumber, InboundPacket inbound) {
      if (bufferedBySeq.containsKey(sequenceNumber)) {
        return false;
      }

      bufferedBySeq.put(sequenceNumber, inbound);
      return true;
    }

    InboundPacket pollNextInOrder() {
      var nextEntry = bufferedBySeq.pollFirstEntry();
      if (nextEntry == null || nextEntry.getKey() != nextExpectedSeq) {
        throw new IllegalStateException("No in-order message available for delivery");
      }

      InboundPacket delivered = nextEntry.getValue();
      if (nextExpectedSeq > Dpch.MAX_PACKET_NUMBER) {
        throw new IllegalStateException("Receiver sequence number exhausted for stream");
      }

      nextExpectedSeq++;
      return delivered;
    }

    boolean hasNextInOrderReady() {
      return !bufferedBySeq.isEmpty() && bufferedBySeq.firstKey() == nextExpectedSeq;
    }

    int nextExpectedSequence() {
      return nextExpectedSeq;
    }

    boolean isStale(long now, long ttlMs) {
      synchronized (this) {
        return bufferedBySeq.isEmpty() && TimeUtil.hasElapsedAtLeast(now, lastTouchedAtMs, ttlMs);
      }
    }
  }
}
