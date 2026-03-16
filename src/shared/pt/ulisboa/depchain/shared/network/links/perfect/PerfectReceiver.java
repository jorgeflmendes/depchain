package pt.ulisboa.depchain.shared.network.links.perfect;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.ulisboa.depchain.proto.DpchPacket;
import pt.ulisboa.depchain.proto.DpchPacketType;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedKey;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.network.packet.DpchPacketUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class PerfectReceiver {
  private static final Logger logger = LoggerFactory.getLogger(PerfectReceiver.class);
  private final PerfectContext context;
  private final PerfectSender sender;

  PerfectReceiver(PerfectContext context, PerfectSender sender) {
    ValidationUtils.requireAllNonNull(named("context", context), named("sender", sender));
    this.context = context;
    this.sender = sender;
  }

  void runInboundLoop() {
    while (context.isRunning()) {
      try {
        InboundPacket inbound = PerfectContext.decodeInboundPacket(context.stubbornLink.receive());
        if (inbound == null) {
          if (!context.isRunning()) {
            break;
          }
          continue;
        }
        receivePacket(inbound);
      } catch (IOException exception) {
        if (!context.isRunning()) {
          break;
        }
        logger.debug("PerfectLink worker error", exception);
      }
    }
  }

  private void receivePacket(InboundPacket inbound) {
    DpchPacket packet = inbound.packet();
    InetSocketAddress remote = inbound.sender();
    ConnectionKey key = new ConnectionKey(remote, packet.getConnectionId());

    DpchPacketType reliableType = DpchPacketUtil.reliableTypeOrNull(packet);
    if (DpchPacketUtil.hasType(packet, DpchPacketType.DPCH_PACKET_TYPE_ACK)) {
      handleAck(packet, reliableType, key, remote);
    }
    if (reliableType != null) {
      handleReliable(inbound, packet, reliableType, key);
    }
  }

  private void handleAck(DpchPacket ackPacket, DpchPacketType fallbackAcknowledgedType, ConnectionKey key, InetSocketAddress remote) {
    DpchPacketType acknowledgedType = PerfectContext.decodeAcknowledgedType(ackPacket.getPayload().toByteArray(), fallbackAcknowledgedType);
    int acknowledgedSequence = ackPacket.getSequenceNumber();
    if (acknowledgedType == null || acknowledgedSequence < 0) {
      return;
    }

    List<TrackedKey> cancellations = new ArrayList<>(1);
    // Resolve ACKs against the current sender state without racing with cleanup.
    context.connectionStates.computeIfPresent(key, (ignored, connectionState) -> {
      cancellations.addAll(connectionState.senderState().acknowledge(ackPacket.getConnectionId(), acknowledgedSequence, acknowledgedType));
      return connectionState;
    });
    context.cancelTrackedBatch(cancellations, remote);
  }

  private void handleReliable(InboundPacket inbound, DpchPacket packet, DpchPacketType reliableType, ConnectionKey key) {
    List<InboundPacket> readyToDeliver = new ArrayList<>(1);
    boolean shouldAckData = false;
    PerfectConnectionState connectionState = context.connectionStates.computeIfAbsent(key, ignored -> new PerfectConnectionState());
    ReceiverState receiverState = connectionState.receiverState();

    synchronized (receiverState) {
      int sequenceNumber = packet.getSequenceNumber();
      if (sequenceNumber < 0) {
        return;
      }

      if (receiverState.isAlreadyDelivered(sequenceNumber)) {
        if (reliableType == DpchPacketType.DPCH_PACKET_TYPE_DATA) {
          shouldAckData = true;
        } else {
          readyToDeliver.add(inbound);
        }
      } else {
        if (reliableType == DpchPacketType.DPCH_PACKET_TYPE_DATA) {
          shouldAckData = true;
        }
        if (receiverState.bufferIfNew(sequenceNumber, inbound)) {
          while (receiverState.hasNextInOrderReady()) {
            readyToDeliver.add(receiverState.pollNextInOrder());
          }
        }
      }
    }

    for (InboundPacket delivered : readyToDeliver) {
      context.offer(delivered);
    }

    if (shouldAckData) {
      sender.sendAckBestEffort(packet.getConnectionId(), packet.getSequenceNumber(), inbound.sender(), PerfectContext.ACK_DATA);
    }
  }
}
