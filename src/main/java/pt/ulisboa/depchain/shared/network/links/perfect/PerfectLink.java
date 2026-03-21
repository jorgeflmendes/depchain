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
import pt.ulisboa.depchain.shared.network.links.LinkClosedException;
import pt.ulisboa.depchain.shared.network.links.fairloss.FairLossLink;
import pt.ulisboa.depchain.shared.network.links.fairloss.InboundBytes;
import pt.ulisboa.depchain.shared.network.links.stubborn.StubbornLink;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedKey;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.TimeUtil;

public final class PerfectLink implements BlockingLink<InboundPacket> {
  private static final Logger logger = LoggerFactory.getLogger(PerfectLink.class);

  public static final int MAX_PACKET_SIZE = FairLossLink.MAX_PACKET_SIZE;

  private final PerfectContext context;
  private final PerfectSender sender;

  public PerfectLink(StubbornLink stubbornLink) {
    this.context = new PerfectContext(stubbornLink);
    this.sender = new PerfectSender(context);
  }

  public static PerfectLink bind(InetSocketAddress bindEndpoint) throws IOException {
    return new PerfectLink(StubbornLink.bind(bindEndpoint));
  }

  public static PerfectLink unbound() throws IOException {
    return new PerfectLink(StubbornLink.unbound());
  }

  public void send(long connectionId, byte[] payload, InetSocketAddress remoteEndpoint) {
    sender.sendData(connectionId, payload, remoteEndpoint);
  }

  @Override
  public InboundPacket receive() throws Exception {
    InboundPacket ready = context.pollReady();
    if (ready != null) {
      return ready;
    }

    while (context.isRunning()) {
      InboundPacket delivered = processDatagram(context.stubbornLink.receive());
      if (delivered != null) {
        return delivered;
      }
    }

    ready = context.pollReady();
    if (ready != null) {
      return ready;
    }
    throw new LinkClosedException("PerfectLink is closed");
  }

  @Override
  public @Nullable InboundPacket receive(long timeoutMs) throws Exception {
    InboundPacket ready = context.pollReady();
    if (ready != null) {
      return ready;
    }

    long deadlineMs = TimeUtil.deadlineAfter(TimeUtil.nowMs(), timeoutMs);
    while (context.isRunning()) {
      long remainingMs = TimeUtil.remainingMsUntil(deadlineMs);
      if (remainingMs <= 0L) {
        return null;
      }

      InboundPacket delivered = processDatagram(context.stubbornLink.receive(remainingMs));
      if (delivered != null) {
        return delivered;
      }
    }

    return context.pollReady();
  }

  public boolean awaitNoPendingData(long connectionId, InetSocketAddress remoteEndpoint, long timeoutMs) throws InterruptedException {
    return sender.awaitNoPendingData(connectionId, remoteEndpoint, timeoutMs);
  }

  public void cancelPendingData(long connectionId, InetSocketAddress remoteEndpoint) {
    sender.cancelPendingData(connectionId, remoteEndpoint);
  }

  public void releaseConnection(long connectionId, InetSocketAddress remoteEndpoint) {
    ConnectionKey connectionKey = new ConnectionKey(remoteEndpoint, connectionId);
    PerfectConnectionState connectionState = context.connectionStates.remove(connectionKey);
    if (connectionState != null) {
      for (TrackedKey trackedKey : connectionState.senderState().cancelPendingData(connectionId)) {
        context.stubbornLink.cancelTracked(trackedKey, remoteEndpoint);
      }
    }
  }

  @Override
  public void close() throws Exception {
    if (!context.stop()) {
      return;
    }

    context.shutdown();
    context.stubbornLink.close();
  }

  private @Nullable InboundPacket processDatagram(@Nullable InboundBytes datagram) throws IOException {
    if (datagram == null) {
      return null;
    }

    try {
      InboundPacket inbound = PerfectContext.decodeInboundPacket(datagram);
      if (inbound == null) {
        return null;
      }
      return receivePacket(inbound);
    } catch (IOException exception) {
      if (!context.isRunning()) {
        return null;
      }
      logger.debug("PerfectLink receive error", exception);
      return null;
    }
  }

  private @Nullable InboundPacket receivePacket(InboundPacket inbound) {
    DpchPacket packet = inbound.packet();
    InetSocketAddress remoteEndpoint = inbound.sender();
    ConnectionKey connectionKey = new ConnectionKey(remoteEndpoint, packet.getConnectionId());

    if (packet.getPacketType() == DpchPacketType.DPCH_PACKET_TYPE_ACK) {
      handleAck(packet, connectionKey, remoteEndpoint);
      return null;
    }
    if (packet.getPacketType() == DpchPacketType.DPCH_PACKET_TYPE_DATA) {
      return handleData(inbound, packet, connectionKey);
    }
    return null;
  }

  private void handleAck(DpchPacket ackPacket, ConnectionKey connectionKey, InetSocketAddress remoteEndpoint) {
    int acknowledgedSequence = ackPacket.getSequenceNumber();
    if (acknowledgedSequence < 0) {
      return;
    }

    PerfectConnectionState connectionState = context.connectionStates.get(connectionKey);
    if (connectionState == null) {
      return;
    }

    TrackedKey trackedKey = connectionState.senderState().acknowledge(ackPacket.getConnectionId(), acknowledgedSequence);
    if (trackedKey != null) {
      context.stubbornLink.cancelTracked(trackedKey, remoteEndpoint);
    }
  }

  private @Nullable InboundPacket handleData(InboundPacket inbound, DpchPacket packet, ConnectionKey connectionKey) {
    InboundPacket firstReady = null;
    List<InboundPacket> overflowReady = null;
    PerfectConnectionState connectionState = context.connectionStates.computeIfAbsent(connectionKey, ignored -> new PerfectConnectionState());
    ReceiverState receiverState = connectionState.receiverState();

    synchronized (receiverState) {
      int sequenceNumber = packet.getSequenceNumber();
      if (sequenceNumber < 0) {
        return null;
      }

      if (!receiverState.isAlreadyDelivered(sequenceNumber) && receiverState.bufferIfNew(sequenceNumber, inbound)) {
        while (receiverState.hasNextInOrderReady()) {
          InboundPacket readyPacket = receiverState.pollNextInOrder();
          if (firstReady == null) {
            firstReady = readyPacket;
          } else {
            if (overflowReady == null) {
              overflowReady = new ArrayList<>(1);
            }
            overflowReady.add(readyPacket);
          }
        }
      }
    }

    if (overflowReady != null) {
      overflowReady.forEach(context::offerReady);
    }
    sender.sendAck(packet.getConnectionId(), packet.getSequenceNumber(), inbound.sender());
    return firstReady;
  }
}
