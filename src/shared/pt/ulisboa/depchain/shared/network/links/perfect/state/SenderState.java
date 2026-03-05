package pt.ulisboa.depchain.shared.network.links.perfect.state;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.dpch.DpchType;
import pt.ulisboa.depchain.shared.network.links.stubborn.model.TrackedMessage;

// State of the sender side of a perfect link, tracking in-flight messages and their sequence numbers.
public final class SenderState {
  private int nextSequence;
  private final NavigableMap<Integer, DpchType> inFlightBySeq = new TreeMap<>();
  private volatile long lastTouchedAtMs = System.currentTimeMillis();

  // Get the next sequence number for a new message of the given type and track it.
  public synchronized int nextSequence(DpchType type, long now) {
    if (nextSequence > Dpch.MAX_PACKET_NUMBER) {
      throw new IllegalStateException("Sender sequence number exhausted for stream");
    }
    int sequence = nextSequence++;
    inFlightBySeq.put(sequence, type);
    touch(now);
    notifyAll();
    return sequence;
  }

  // Acknowledge the message with the given sequence number and type, returning any messages that should be cancelled as a result.
  public synchronized List<TrackedMessage.Key> acknowledge(long connectionId, int sequenceNumber, DpchType acknowledgedType, long now) {
    touch(now);
    List<TrackedMessage.Key> cancellations = new ArrayList<>();

    if (acknowledgedType == DpchType.FIN) { // TODO: maybe this should be moved to the handshaked link
      Iterator<Map.Entry<Integer, DpchType>> iterator = inFlightBySeq.headMap(sequenceNumber, true).entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<Integer, DpchType> entry = iterator.next();
        cancellations.add(new TrackedMessage.Key(connectionId, entry.getKey(), Byte.toUnsignedInt(entry.getValue().code())));
        iterator.remove();
      }
      notifyAll();
      return cancellations;
    }

    // Only acknowledge the message if it matches the expected type for the sequence number.
    DpchType trackedType = inFlightBySeq.get(sequenceNumber);
    if (trackedType == acknowledgedType) {
      inFlightBySeq.remove(sequenceNumber);
      cancellations.add(new TrackedMessage.Key(connectionId, sequenceNumber, Byte.toUnsignedInt(acknowledgedType.code())));
      notifyAll();
    }

    return cancellations;
  }

  public void touch(long now) {
    lastTouchedAtMs = now;
  }

  // Check if there are any pending messages of the given type.
  synchronized boolean hasPending(DpchType type) {
    for (DpchType inFlightType : inFlightBySeq.values()) {
      if (inFlightType == type) {
        return true;
      }
    }
    return false;
  }

  // Wait until there are no pending messages of the given type, or until the deadline is reached.
  public synchronized boolean waitUntilNoPending(DpchType type, long deadlineMs) throws InterruptedException {
    while (hasPending(type)) {
      long remainingMs = deadlineMs - System.currentTimeMillis();
      if (remainingMs <= 0L) {
        return false;
      }
      wait(remainingMs);
    }
    return true;
  }

  public boolean isStale(long now, long ttlMs) {
    return (now - lastTouchedAtMs) >= ttlMs;
  }
}
