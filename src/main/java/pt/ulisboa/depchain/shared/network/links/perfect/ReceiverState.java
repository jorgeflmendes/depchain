package pt.ulisboa.depchain.shared.network.links.perfect;

import java.util.HashMap;
import java.util.Map;

import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.network.packet.DpchPacketUtil;

final class ReceiverState {
  private int nextExpectedSeq;
  private final Map<Integer, InboundPacket> bufferedBySeq = new HashMap<>();

  ReceiverState(int nextExpectedSeq) {
    this.nextExpectedSeq = Math.max(0, nextExpectedSeq);
  }

  boolean isAlreadyDelivered(int sequenceNumber) {
    return sequenceNumber < nextExpectedSeq;
  }

  boolean bufferIfNew(int sequenceNumber, InboundPacket inbound) {
    return bufferedBySeq.putIfAbsent(sequenceNumber, inbound) == null;
  }

  InboundPacket pollNextInOrder() {
    InboundPacket delivered = bufferedBySeq.remove(nextExpectedSeq);
    if (delivered == null) {
      throw new IllegalStateException("No in-order message available for delivery");
    }

    if (nextExpectedSeq > DpchPacketUtil.MAX_PACKET_NUMBER) {
      throw new IllegalStateException("Receiver sequence number exhausted for stream");
    }

    nextExpectedSeq++;
    return delivered;
  }

  boolean hasNextInOrderReady() {
    return bufferedBySeq.containsKey(nextExpectedSeq);
  }
}
