package pt.ulisboa.depchain.shared.network.links.perfect;

import java.util.NavigableMap;
import java.util.TreeMap;

import pt.ulisboa.depchain.shared.network.packet.DpchPacket;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;

final class ReceiverState {
  private int nextExpectedSeq;
  private final NavigableMap<Integer, InboundPacket> bufferedBySeq = new TreeMap<>();

  ReceiverState(int nextExpectedSeq) {
    this.nextExpectedSeq = Math.max(0, nextExpectedSeq);
  }

  boolean isAlreadyDelivered(int sequenceNumber) {
    return sequenceNumber < nextExpectedSeq;
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
    if (nextExpectedSeq > DpchPacket.MAX_PACKET_NUMBER) {
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
}
