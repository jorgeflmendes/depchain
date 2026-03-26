package pt.ulisboa.depchain.shared.network.links.perfect;

import java.util.HashMap;
import java.util.Map;

import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.network.packet.PacketLimits;

final class ReceiverState {
  private int nextExpectedSeq;
  private final Map<Integer, InboundPacket> bufferedBySeq = new HashMap<>();

  ReceiverState(int nextExpectedSeq) {
    this.nextExpectedSeq = Math.max(0, nextExpectedSeq);
  }

  boolean isAlreadyDelivered(int sequenceNumber) {
    return sequenceNumber < nextExpectedSeq;
  }

  boolean isNextExpected(int sequenceNumber) {
    return sequenceNumber == nextExpectedSeq;
  }

  boolean bufferIfNew(int sequenceNumber, InboundPacket inbound) {
    return bufferedBySeq.putIfAbsent(sequenceNumber, inbound) == null;
  }

  void markNextDelivered() {
    if (nextExpectedSeq > PacketLimits.MAX_PACKET_NUMBER) {
      throw new IllegalStateException("Receiver sequence number exhausted for stream");
    }
    nextExpectedSeq++;
  }

  InboundPacket pollBufferedNextInOrder() {
    InboundPacket delivered = bufferedBySeq.remove(nextExpectedSeq);
    if (delivered == null) {
      return null;
    }

    markNextDelivered();
    return delivered;
  }
}
