package pt.ulisboa.depchain.shared.network.links.perfect;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.ulisboa.depchain.proto.DpchPacketType;
import pt.ulisboa.depchain.shared.network.packet.DpchPacketUtil;
import pt.ulisboa.depchain.shared.network.links.LinkFailureException;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedKey;
import pt.ulisboa.depchain.shared.utils.TimeUtil;

final class SenderState {
  private static final Logger logger = LoggerFactory.getLogger(SenderState.class);
  private static final int NEAR_EXHAUSTION_THRESHOLD = 1024;

  private int nextSequence;
  private final NavigableMap<Integer, DpchPacketType> inFlightBySeq = new TreeMap<>();
  private boolean nearExhaustionLogged;

  synchronized int nextOrPendingSequence(DpchPacketType type, boolean reusePendingSameType) {
    if (reusePendingSameType) {
      for (Map.Entry<Integer, DpchPacketType> entry : inFlightBySeq.entrySet()) {
        if (entry.getValue() == type) {
          return entry.getKey();
        }
      }
    }

    if (!nearExhaustionLogged && nextSequence >= DpchPacketUtil.MAX_PACKET_NUMBER - NEAR_EXHAUSTION_THRESHOLD) {
      nearExhaustionLogged = true;
      logger.warn("Sender sequence number nearing exhaustion for stream. nextSequence={}, inFlight={}", nextSequence, inFlightBySeq.size());
    }

    if (nextSequence > DpchPacketUtil.MAX_PACKET_NUMBER) {
      throw new IllegalStateException("Sender sequence number exhausted for stream");
    }

    int sequence = nextSequence++;
    inFlightBySeq.put(sequence, type);
    notifyAll();
    return sequence;
  }

  synchronized List<TrackedKey> acknowledge(long connectionId, int sequenceNumber, DpchPacketType acknowledgedType) {
    List<TrackedKey> cancellations = new ArrayList<>();

    if (acknowledgedType == DpchPacketType.DPCH_PACKET_TYPE_FIN) {
      Iterator<Map.Entry<Integer, DpchPacketType>> iterator = inFlightBySeq.headMap(sequenceNumber, true).entrySet().iterator();

      while (iterator.hasNext()) {
        Map.Entry<Integer, DpchPacketType> entry = iterator.next();
        cancellations.add(new TrackedKey(connectionId, entry.getKey(), entry.getValue().getNumber()));
        iterator.remove();
      }

      notifyAll();
      return cancellations;
    }

    DpchPacketType trackedType = inFlightBySeq.get(sequenceNumber);
    if (trackedType == acknowledgedType) {
      inFlightBySeq.remove(sequenceNumber);
      cancellations.add(new TrackedKey(connectionId, sequenceNumber, acknowledgedType.getNumber()));
      notifyAll();
    }

    return cancellations;
  }

  synchronized List<TrackedKey> cancelPendingType(long connectionId, DpchPacketType packetType) {
    List<TrackedKey> cancellations = new ArrayList<>();
    Iterator<Map.Entry<Integer, DpchPacketType>> iterator = inFlightBySeq.entrySet().iterator();

    while (iterator.hasNext()) {
      Map.Entry<Integer, DpchPacketType> entry = iterator.next();
      if (entry.getValue() != packetType) {
        continue;
      }

      cancellations.add(new TrackedKey(connectionId, entry.getKey(), packetType.getNumber()));
      iterator.remove();
    }

    notifyAll();
    return cancellations;
  }

  synchronized List<TrackedKey> takeAllTracked(long connectionId) {
    List<TrackedKey> cancellations = new ArrayList<>(inFlightBySeq.size());
    for (Map.Entry<Integer, DpchPacketType> entry : inFlightBySeq.entrySet()) {
      cancellations.add(new TrackedKey(connectionId, entry.getKey(), entry.getValue().getNumber()));
    }

    inFlightBySeq.clear();
    notifyAll();
    return cancellations;
  }

  synchronized boolean waitUntilNoPending(PerfectContext context, long connectionId, InetSocketAddress remoteEndpoint, DpchPacketType type, long deadlineMs) throws InterruptedException {
    while (context.isRunning() && hasPending(type)) {
      LinkFailureException failure = pollTerminalFailureForType(context, connectionId, remoteEndpoint, type);
      if (failure != null) {
        throw failure;
      }

      long remainingMs = TimeUtil.remainingMsUntil(deadlineMs);
      if (remainingMs <= 0L) {
        return false;
      }
      wait(remainingMs);
    }
    return !hasPending(type);
  }

  synchronized void notifyWaiters() {
    notifyAll();
  }

  private boolean hasPending(DpchPacketType type) {
    for (DpchPacketType inFlightType : inFlightBySeq.values()) {
      if (inFlightType == type) {
        return true;
      }
    }
    return false;
  }

  synchronized LinkFailureException pollTerminalFailureForType(PerfectContext context, long connectionId, InetSocketAddress remoteEndpoint, DpchPacketType type) {
    for (Map.Entry<Integer, DpchPacketType> entry : inFlightBySeq.entrySet()) {
      if (entry.getValue() != type) {
        continue;
      }

      LinkFailureException failure = context.pollTerminalFailure(connectionId, remoteEndpoint, entry.getKey(), entry.getValue());
      if (failure != null) {
        return failure;
      }
    }

    return null;
  }
}

