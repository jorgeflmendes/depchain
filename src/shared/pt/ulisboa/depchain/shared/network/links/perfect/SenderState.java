package pt.ulisboa.depchain.shared.network.links.perfect;

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
import pt.ulisboa.depchain.shared.utils.TimeUtil;

final class SenderState {
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

  synchronized boolean waitUntilNoPending(PerfectContext context, long connectionId, InetSocketAddress remoteEndpoint, DpchType type, long deadlineMs) throws InterruptedException {
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
