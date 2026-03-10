package pt.ulisboa.depchain.shared.network.links.perfect;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import pt.ulisboa.depchain.shared.logging.Logger;
import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.dpch.DpchType;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedKey;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class PerfectReceiver {
  private static final Logger logger = new Logger("PerfectReceiver");
  private final PerfectContext context;
  private final PerfectSender sender;

  PerfectReceiver(PerfectContext context, PerfectSender sender) {
    ValidationUtils.requireAllNonNull(named("context", context), named("sender", sender));
    this.context = context;
    this.sender = sender;
  }

  void runInboundLoop() {
    while (context.running.get()) {
      try {
        handleInbound(PerfectContext.decodeInboundPacket(context.stubbornLink.receive()));
      } catch (IllegalStateException exception) {
        if (!context.running.get()) {
          logger.warn("Ignoring PerfectLink shutdown race: " + exception.getMessage());
          break;
        }
        throw exception;
      } catch (IOException exception) {
        if (!context.running.get()) {
          break;
        }
        logger.debug("PerfectLink worker error: " + exception.getMessage());
      }
    }
  }

  private void handleInbound(InboundPacket inbound) {
    Dpch packet = inbound.packet();
    InetSocketAddress remote = inbound.sender();
    ConnectionKey key = new ConnectionKey(remote, packet.connectionId());

    DpchType reliableType = packet.reliableTypeOrNull();
    if (packet.hasType(DpchType.ACK)) {
      handleAck(packet, reliableType, key, remote);
    }
    if (reliableType != null) {
      handleReliable(inbound, packet, reliableType, key);
    }
  }

  private void handleAck(Dpch ackPacket, DpchType fallbackAcknowledgedType, ConnectionKey key, InetSocketAddress remote) {
    DpchType acknowledgedType = PerfectContext.decodeAcknowledgedType(ackPacket.payload(), fallbackAcknowledgedType);
    int acknowledgedSequence = ackPacket.sequenceNumber();
    if (acknowledgedType == null || acknowledgedSequence < 0) {
      return;
    }

    List<TrackedKey> cancellations = new java.util.ArrayList<>();
    // Resolve ACKs against the current sender state without racing with cleanup.
    context.sendSequences.computeIfPresent(key, (ignored, senderState) -> {
      cancellations.addAll(senderState.acknowledge(ackPacket.connectionId(), acknowledgedSequence, acknowledgedType));
      return senderState;
    });
    for (TrackedKey trackedKey : cancellations) {
      context.stubbornLink.cancelTracked(trackedKey, remote);
    }
  }

  private void handleReliable(InboundPacket inbound, Dpch packet, DpchType reliableType, ConnectionKey key) {
    List<InboundPacket> readyToDeliver = new java.util.ArrayList<>();
    boolean[] shouldAckData = new boolean[1];

    // Create or reuse the receiver state atomically for this stream.
    context.receiverStates.compute(key, (ignored, existingState) -> {
      ReceiverState receiverState = existingState;
      if (receiverState == null) {
        receiverState = new ReceiverState(0);
      }

      synchronized (receiverState) {
        int sequenceNumber = packet.sequenceNumber();
        if (sequenceNumber < 0) {
          return receiverState;
        }

        if (receiverState.isAlreadyDelivered(sequenceNumber)) {
          if (reliableType == DpchType.DATA) {
            shouldAckData[0] = true;
          } else {
            readyToDeliver.add(inbound);
          }
        } else {
          if (reliableType == DpchType.DATA) {
            shouldAckData[0] = true;
          }
          if (receiverState.bufferIfNew(sequenceNumber, inbound)) {
            while (receiverState.hasNextInOrderReady()) {
              readyToDeliver.add(receiverState.pollNextInOrder());
            }
          }
        }
      }

      return receiverState;
    });

    for (InboundPacket delivered : readyToDeliver) {
      context.deliveryQueue.offer(delivered);
    }

    if (shouldAckData[0]) {
      sender.sendAckBestEffort(packet.connectionId(), packet.sequenceNumber(), inbound.sender(), PerfectContext.ACK_DATA);
    }
  }
}
