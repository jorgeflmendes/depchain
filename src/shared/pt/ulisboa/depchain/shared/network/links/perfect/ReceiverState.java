package pt.ulisboa.depchain.shared.network.links.perfect;

import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

import pt.ulisboa.depchain.shared.network.model.InboundMessage;

final class ReceiverState {
  // The next expected sequence number to be delivered in order
  private int nextExpectedSeq = 0;

  // Used for staleness checks
  private volatile long lastTouchedAtMs = System.currentTimeMillis();

  // Buffered out-of-order messages
  private final Set<Integer> bufferedMessageSeqNums = new HashSet<>();

  // To hold buffered messages in order of their sequence numbers
  private final PriorityQueue<InboundMessage> buffer = new PriorityQueue<>((a, b) -> Integer.compare(a.packet().sequenceNumber(), b.packet().sequenceNumber()));

  void touch(long now) {
    lastTouchedAtMs = now;
  }

  boolean isAlreadyDelivered(int sequenceNumber) {
    return sequenceNumber < nextExpectedSeq;
  }

  boolean isOutsideWindow(int sequenceNumber, int maxWindowSize) {
    return sequenceNumber >= nextExpectedSeq + maxWindowSize;
  }

  // Returns true if the message was buffered, false if it was already buffered or delivered.
  boolean bufferIfNew(int sequenceNumber, InboundMessage inbound) {
    if (!bufferedMessageSeqNums.add(sequenceNumber)) {
      return false;
    }
    buffer.offer(inbound);
    return true;
  }

  boolean hasNextInOrderReady() {
    return !buffer.isEmpty() && buffer.peek().packet().sequenceNumber() == nextExpectedSeq;
  }

  // Removes and returns the next in-order message.
  InboundMessage pollNextInOrder() {
    InboundMessage delivered = buffer.poll();
    bufferedMessageSeqNums.remove(delivered.packet().sequenceNumber());
    nextExpectedSeq++;
    return delivered;
  }

  // If it has no buffered messages and has not been touched for a while.
  boolean isStale(long now, long ttlMs) {
    synchronized (this) {
      return buffer.isEmpty() && (now - lastTouchedAtMs) >= ttlMs;
    }
  }
}
