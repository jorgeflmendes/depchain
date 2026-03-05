package pt.ulisboa.depchain.shared.network.links.perfect.state;

import java.util.NavigableMap;
import java.util.TreeMap;

import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.model.InboundMessage;

// State of the receiver side of a perfect link, tracking the next expected sequence number and any buffered out-of-order messages.
public final class ReceiverState {
  private int nextExpectedSeq;
  private volatile long lastTouchedAtMs = System.currentTimeMillis();
  private final NavigableMap<Integer, InboundMessage> bufferedBySeq = new TreeMap<>();

  public ReceiverState(int nextExpectedSeq) {
    this.nextExpectedSeq = Math.max(0, nextExpectedSeq);
  }

  public void touch(long now) {
    lastTouchedAtMs = now;
  }

  public boolean isAlreadyDelivered(int sequenceNumber) {
    return sequenceNumber < nextExpectedSeq;
  }
  
  public boolean isOutsideWindow(int sequenceNumber, int maxWindowSize) {
    return (long) sequenceNumber >= (long) nextExpectedSeq + maxWindowSize;
  }

  // Buffer an out-of-order message if it's not already buffered
  public boolean bufferIfNew(int sequenceNumber, InboundMessage inbound) {
    if (bufferedBySeq.containsKey(sequenceNumber)) {
      return false;
    }

    bufferedBySeq.put(sequenceNumber, inbound);
    return true;
  }

  // Poll the next in-order message for delivery, throwing if it's not available or if the sequence number is bigger than the maximum allowed.
  public InboundMessage pollNextInOrder() {
    var nextEntry = bufferedBySeq.pollFirstEntry();
    if (nextEntry == null || nextEntry.getKey() != nextExpectedSeq) {
      throw new IllegalStateException("No in-order message available for delivery");
    }
    
    InboundMessage delivered = nextEntry.getValue();
    if (nextExpectedSeq > Dpch.MAX_PACKET_NUMBER) {
      throw new IllegalStateException("Receiver sequence number exhausted for stream");
    }
    
    nextExpectedSeq++;
    return delivered;
  }

  public boolean hasNextInOrderReady() {
    return !bufferedBySeq.isEmpty() && bufferedBySeq.firstKey() == nextExpectedSeq;
  }

  public int nextExpectedSequence() {
    return nextExpectedSeq;
  }

  public boolean isStale(long now, long ttlMs) {
    synchronized (this) {
      return bufferedBySeq.isEmpty() && (now - lastTouchedAtMs) >= ttlMs;
    }
  }
}
