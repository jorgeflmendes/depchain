package pt.ulisboa.depchain.shared.network.links.perfect;

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
    this.context = ValidationUtils.requireNonNull(context, "context");
    this.sender = ValidationUtils.requireNonNull(sender, "sender");
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
    PerfectSender.SenderState senderState = context.sendSequences.get(key);
    if (senderState == null) {
      return;
    }

    DpchType acknowledgedType = PerfectContext.decodeAcknowledgedType(ackPacket.payload(), fallbackAcknowledgedType);
    int acknowledgedSequence = ackPacket.sequenceNumber();
    if (acknowledgedType == null || acknowledgedSequence < 0) {
      return;
    }

    List<TrackedKey> cancellations = senderState.acknowledge(ackPacket.connectionId(), acknowledgedSequence, acknowledgedType, now);
    for (TrackedKey trackedKey : cancellations) {
      context.stubbornLink.cancelTracked(trackedKey, remote);
    }
  }

  private void handleReliable(InboundPacket inbound, Dpch packet, DpchType reliableType, ConnectionKey key, long now) {
    ReceiverState state = context.receiverStates.computeIfAbsent(key, ignored -> {
          Integer persisted = context.deliveredSequenceFloors.remove(key);
          if (persisted == null) {
            return new ReceiverState(0);
          }
          return new ReceiverState(persisted);
        });

    boolean shouldAckData = false;
    synchronized (state) {
      state.touch(now);
      int sequenceNumber = packet.sequenceNumber();
      if (sequenceNumber < 0) {
        return;
      }

      if (state.isAlreadyDelivered(sequenceNumber)) {
        if (reliableType == DpchType.DATA) {
          shouldAckData = true;
        } else {
          context.deliveryQueue.offer(inbound);
        }
      } else if (!state.isOutsideWindow(sequenceNumber, context.maxWindowSize)) {
        if (reliableType == DpchType.DATA) {
          shouldAckData = true;
        }
        if (state.bufferIfNew(sequenceNumber, inbound)) {
          while (state.hasNextInOrderReady()) {
            context.deliveryQueue.offer(state.pollNextInOrder());
          }
        }
      }
    }

    if (shouldAckData) {
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
