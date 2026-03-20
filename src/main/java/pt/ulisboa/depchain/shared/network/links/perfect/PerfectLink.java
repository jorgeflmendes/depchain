package pt.ulisboa.depchain.shared.network.links.perfect;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.eclipse.jdt.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.ulisboa.depchain.proto.DpchPacket;
import pt.ulisboa.depchain.proto.DpchPacketType;
import pt.ulisboa.depchain.shared.network.links.BlockingLink;
import pt.ulisboa.depchain.shared.network.links.LinkThreadUtil;
import pt.ulisboa.depchain.shared.network.links.fairloss.FairLossLink;
import pt.ulisboa.depchain.shared.network.links.stubborn.StubbornLink;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedKey;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.network.packet.DpchPacketUtil;

public final class PerfectLink implements BlockingLink<InboundPacket> {
  private static final Logger logger = LoggerFactory.getLogger(PerfectLink.class);

  public static final int MAX_PACKET_SIZE = FairLossLink.MAX_PACKET_SIZE;
  private static final byte[] EMPTY_CONTROL_PAYLOAD = new byte[0];

  private final PerfectContext context;
  private final PerfectSender sender;
  private final Thread workerThread;

  public PerfectLink(StubbornLink stubbornLink) {
    this.context = new PerfectContext(stubbornLink);
    this.sender = new PerfectSender(context);
    this.workerThread = Thread.ofVirtual().name("perfect-link").start(this::runInboundLoop);
  }

  public static PerfectLink bind(InetSocketAddress bindEndpoint) throws IOException {
    StubbornLink stubbornLink = StubbornLink.bind(bindEndpoint);
    return new PerfectLink(stubbornLink);
  }

  public static PerfectLink unbound() throws IOException {
    StubbornLink stubbornLink = StubbornLink.unbound();
    return new PerfectLink(stubbornLink);
  }

  public void sendData(long connectionId, byte[] payload, InetSocketAddress remoteEndpoint) {
    sender.send(connectionId, DpchPacketType.DPCH_PACKET_TYPE_DATA, false, payload, remoteEndpoint);
  }

  public void sendSyn(long connectionId, InetSocketAddress remoteEndpoint) {
    sender.send(connectionId, DpchPacketType.DPCH_PACKET_TYPE_SYN, false, EMPTY_CONTROL_PAYLOAD, remoteEndpoint);
  }

  public void sendSynAck(long connectionId, InetSocketAddress remoteEndpoint) {
    sender.send(connectionId, DpchPacketType.DPCH_PACKET_TYPE_SYN, true, EMPTY_CONTROL_PAYLOAD, remoteEndpoint);
  }

  public void sendFin(long connectionId, InetSocketAddress remoteEndpoint) {
    sender.send(connectionId, DpchPacketType.DPCH_PACKET_TYPE_FIN, false, EMPTY_CONTROL_PAYLOAD, remoteEndpoint);
  }

  public void sendFinAck(long connectionId, InetSocketAddress remoteEndpoint) {
    sender.send(connectionId, DpchPacketType.DPCH_PACKET_TYPE_FIN, true, EMPTY_CONTROL_PAYLOAD, remoteEndpoint);
  }

  public void sendAck(long connectionId, int acknowledgedSequence, DpchPacketType acknowledgedType, InetSocketAddress remoteEndpoint) {
    sender.sendAck(connectionId, acknowledgedSequence, acknowledgedType, remoteEndpoint);
  }

  @Override
  public InboundPacket receive() throws InterruptedException {
    return context.receive();
  }

  @Override
  public @Nullable InboundPacket receive(long timeoutMs) throws InterruptedException {
    return context.receive(timeoutMs);
  }

  public boolean awaitNoPendingSyn(long connectionId, InetSocketAddress remoteEndpoint, long timeoutMs) throws InterruptedException {
    return sender.awaitNoPendingType(connectionId, remoteEndpoint, DpchPacketType.DPCH_PACKET_TYPE_SYN, timeoutMs);
  }

  public boolean awaitNoPendingFin(long connectionId, InetSocketAddress remoteEndpoint, long timeoutMs) throws InterruptedException {
    return sender.awaitNoPendingType(connectionId, remoteEndpoint, DpchPacketType.DPCH_PACKET_TYPE_FIN, timeoutMs);
  }

  public boolean awaitNoPendingData(long connectionId, InetSocketAddress remoteEndpoint, long timeoutMs) throws InterruptedException {
    return sender.awaitNoPendingType(connectionId, remoteEndpoint, DpchPacketType.DPCH_PACKET_TYPE_DATA, timeoutMs);
  }

  public void cancelPendingData(long connectionId, InetSocketAddress remoteEndpoint) {
    sender.cancelPendingType(connectionId, remoteEndpoint, DpchPacketType.DPCH_PACKET_TYPE_DATA);
  }

  public void cancelPendingControl(long connectionId, InetSocketAddress remoteEndpoint) {
    sender.cancelPendingType(connectionId, remoteEndpoint, DpchPacketType.DPCH_PACKET_TYPE_SYN);
    sender.cancelPendingType(connectionId, remoteEndpoint, DpchPacketType.DPCH_PACKET_TYPE_FIN);
  }

  public void releaseConnection(long connectionId, InetSocketAddress remoteEndpoint) {
    ConnectionKey connectionKey = new ConnectionKey(remoteEndpoint, connectionId);
    PerfectConnectionState connectionState = context.connectionStates.remove(connectionKey);
    if (connectionState != null) {
      for (TrackedKey trackedKey : connectionState.senderState().takeAllTracked(connectionId)) {
        context.stubbornLink.cancelTracked(trackedKey, remoteEndpoint);
      }
    }
  }

  @Override
  public void close() throws Exception {
    if (!context.stop()) {
      return;
    }

    try {
      context.shutdown();
      workerThread.interrupt();
      context.stubbornLink.close();
    } finally {
      LinkThreadUtil.awaitStop(workerThread, "perfect-link");
    }
  }

  private void runInboundLoop() {
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
    InetSocketAddress remoteEndpoint = inbound.sender();
    ConnectionKey connectionKey = new ConnectionKey(remoteEndpoint, packet.getConnectionId());

    DpchPacketType reliableType = DpchPacketUtil.reliableTypeOrNull(packet);
    if (DpchPacketUtil.hasType(packet, DpchPacketType.DPCH_PACKET_TYPE_ACK)) {
      handleAck(packet, reliableType, connectionKey, remoteEndpoint);
    }
    if (reliableType != null) {
      handleReliable(inbound, packet, reliableType, connectionKey);
    }
  }

  private void handleAck(DpchPacket ackPacket, DpchPacketType fallbackAcknowledgedType, ConnectionKey connectionKey, InetSocketAddress remoteEndpoint) {
    DpchPacketType acknowledgedType = PerfectContext.decodeAcknowledgedType(ackPacket.getPayload().toByteArray(), fallbackAcknowledgedType);
    int acknowledgedSequence = ackPacket.getSequenceNumber();
    if (acknowledgedType == null || acknowledgedSequence < 0) {
      return;
    }

    List<TrackedKey> cancellations = new ArrayList<>(1);
    context.connectionStates.computeIfPresent(connectionKey, (ignored, connectionState) -> {
      cancellations.addAll(connectionState.senderState().acknowledge(ackPacket.getConnectionId(), acknowledgedSequence, acknowledgedType));
      return connectionState;
    });
    for (TrackedKey trackedKey : cancellations) {
      context.stubbornLink.cancelTracked(trackedKey, remoteEndpoint);
    }
  }

  private void handleReliable(InboundPacket inbound, DpchPacket packet, DpchPacketType reliableType, ConnectionKey connectionKey) {
    List<InboundPacket> readyToDeliver = new ArrayList<>(1);
    boolean shouldAckData = false;
    PerfectConnectionState connectionState = context.connectionStates.computeIfAbsent(connectionKey, ignored -> new PerfectConnectionState());
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

    readyToDeliver.forEach(context::offer);

    if (shouldAckData) {
      sender.sendAckBestEffort(packet.getConnectionId(), packet.getSequenceNumber(), inbound.sender(), PerfectContext.ACK_DATA);
    }
  }
}
