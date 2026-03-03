package pt.ulisboa.depchain.shared.network.links.perfect;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.dpch.DpchType;
import pt.ulisboa.depchain.shared.network.links.stubborn.TrackedMessage;

final class SenderState {
  // Next local sequence number to be sent in this stream.
  private int nextSequence = 0;

  // Outstanding tracked packets still waiting for ACK, indexed by sequence number.
  private final NavigableMap<Integer, DpchType> inFlightBySeq = new TreeMap<>();

  // Timestamp of the last activity on this stream, used for staleness checks.
  private volatile long lastTouchedAtMs = System.currentTimeMillis();

  synchronized int nextSequence(DpchType type, long now) {
    if (nextSequence > Dpch.MAX_PACKET_NUMBER) {
      throw new IllegalStateException("Sender sequence number exhausted for stream");
    }

    int sequence = nextSequence++;
    inFlightBySeq.put(sequence, type);
    touch(now);
    return sequence;
  }

  // Translate one ACK into tracked-message keys that should be canceled in StubbornLink.
  synchronized List<TrackedMessage.Key> acknowledge(long connectionId, int sequenceNumber, DpchType acknowledgedType, long now) {
    touch(now);
    List<TrackedMessage.Key> cancellations = new ArrayList<>();

    if (acknowledgedType == DpchType.FIN) {
      // FIN ACK is treated as cumulative close acknowledgment for everything up to FIN sequence.
      Iterator<Map.Entry<Integer, DpchType>> iterator = inFlightBySeq.headMap(sequenceNumber, true).entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<Integer, DpchType> entry = iterator.next();
        cancellations.add(new TrackedMessage.Key(connectionId, entry.getKey(), entry.getValue()));
        iterator.remove();
      }
      return cancellations;
    }

    DpchType trackedType = inFlightBySeq.get(sequenceNumber);
    if (trackedType == acknowledgedType) {
      inFlightBySeq.remove(sequenceNumber);
      cancellations.add(new TrackedMessage.Key(connectionId, sequenceNumber, acknowledgedType));
    }
    return cancellations;
  }

  void touch(long now) {
    lastTouchedAtMs = now;
  }
  
  boolean isStale(long now, long ttlMs) {
    return (now - lastTouchedAtMs) >= ttlMs;
  }
}
