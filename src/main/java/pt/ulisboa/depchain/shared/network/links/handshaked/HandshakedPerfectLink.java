package pt.ulisboa.depchain.shared.network.links.handshaked;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.eclipse.jdt.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.ulisboa.depchain.proto.DpchPacketType;
import pt.ulisboa.depchain.shared.network.links.BlockingLink;
import pt.ulisboa.depchain.shared.network.links.LinkClosedException;
import pt.ulisboa.depchain.shared.network.links.LinkThreadUtil;
import pt.ulisboa.depchain.shared.network.links.perfect.PerfectLink;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.network.packet.DpchPacketUtil;

public final class HandshakedPerfectLink implements BlockingLink<InboundPacket> {
  private static final Logger logger = LoggerFactory.getLogger(HandshakedPerfectLink.class);
  private record ReceiveDecision(boolean deliver, HandshakeReply reply) {
  }

  private final HandshakedContext context;
  private final HandshakedSender sender;
  private final Thread workerThread;

  private HandshakedPerfectLink(PerfectLink perfectLink) {
    this.context = new HandshakedContext(perfectLink);
    this.sender = new HandshakedSender(context);

    this.workerThread = Thread.ofVirtual().name("handshaked-perfect-link").start(this::runInboundLoop);
  }

  public static HandshakedPerfectLink bind(InetSocketAddress bindEndpoint) throws IOException {
    PerfectLink perfect = PerfectLink.bind(bindEndpoint);
    return new HandshakedPerfectLink(perfect);
  }

  public static HandshakedPerfectLink unbound() throws IOException {
    PerfectLink perfect = PerfectLink.unbound();
    return new HandshakedPerfectLink(perfect);
  }

  public void send(long connectionId, byte[] payload, InetSocketAddress remoteEndpoint) {
    sender.send(connectionId, payload, remoteEndpoint);
  }

  @Override
  public InboundPacket receive() throws InterruptedException {
    return context.receive();
  }

  @Override
  public @Nullable InboundPacket receive(long timeoutMs) throws InterruptedException {
    return context.receive(timeoutMs);
  }

  public void closeConnection(long connectionId, InetSocketAddress remoteEndpoint) {
    sender.closeConnection(connectionId, remoteEndpoint);
  }

  private void runInboundLoop() {
    while (context.isRunning()) {
      try {
        InboundPacket inbound = context.perfectLink.receive();
        if (inbound == null) {
          if (!context.isRunning()) {
            break;
          }
          continue;
        }

        InboundPacket packet = receivePacket(inbound);
        if (packet != null) {
          context.offer(packet);
        }
      } catch (LinkClosedException closed) {
        break;
      } catch (IllegalStateException exception) {
        if (!context.isRunning()) {
          break;
        }
        logger.debug("HandshakedPerfectLink worker error", exception);
      } catch (InterruptedException interrupted) {
        if (!context.isRunning()) {
          break;
        }
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  private InboundPacket receivePacket(InboundPacket inbound) {
    DpchPacketType packetType = inbound.packet().getPacketType();
    if (!handlesInboundType(packetType)) {
      return null;
    }

    long connectionId = inbound.packet().getConnectionId();
    int sequenceNumber = inbound.packet().getSequenceNumber();
    InetSocketAddress remote = inbound.sender();
    ConnectionKey connectionKey = new ConnectionKey(remote, connectionId);
    ConnectionState connectionState = context.getOrCreateConnectionState(connectionKey);
    ReceiveDecision receiveDecision;
    synchronized (connectionState) {
      receiveDecision = decidePacket(connectionState, packetType, DpchPacketUtil.hasType(inbound.packet(), DpchPacketType.DPCH_PACKET_TYPE_ACK));
    }

    sender.sendHandshakeReply(receiveDecision.reply(), connectionId, sequenceNumber, packetType, remote);
    if (packetType == DpchPacketType.DPCH_PACKET_TYPE_FIN) {
      context.perfectLink.cancelPendingData(connectionId, remote);
      context.perfectLink.cancelPendingControl(connectionId, remote);
      synchronized (connectionState) {
        if (connectionState.isCloseConverged()) {
          context.perfectLink.releaseConnection(connectionId, remote);
        }
      }
    }

    if (receiveDecision.deliver()) {
      return inbound;
    }
    return null;
  }

  private static ReceiveDecision decidePacket(ConnectionState connectionState, DpchPacketType packetType, boolean hasAck) {
    if (packetType == DpchPacketType.DPCH_PACKET_TYPE_SYN) {
      return decideSyn(connectionState, hasAck);
    }
    if (packetType == DpchPacketType.DPCH_PACKET_TYPE_FIN) {
      return decideFin(connectionState);
    }
    return new ReceiveDecision(connectionState.canExchangeData(), HandshakeReply.NONE);
  }

  private static ReceiveDecision decideSyn(ConnectionState connectionState, boolean hasAck) {
    connectionState.markRemoteEstablishedIfNotFinished();
    if (connectionState.isClosing()) {
      return new ReceiveDecision(false, HandshakeReply.ACK);
    }
    if (!hasAck) {
      if (connectionState.shouldSendSyn()) {
        connectionState.markLocalEstablished();
      }
      return new ReceiveDecision(false, HandshakeReply.SYN_ACK);
    }

    HandshakeReply reply = HandshakeReply.ACK;
    if (connectionState.shouldSendSyn()) {
      connectionState.markLocalEstablished();
      reply = HandshakeReply.SYN_ACK;
    }
    return new ReceiveDecision(false, reply);
  }

  private static ReceiveDecision decideFin(ConnectionState connectionState) {
    connectionState.markRemoteFinished();
    HandshakeReply reply = HandshakeReply.ACK;
    if (!connectionState.isLocalFinished()) {
      connectionState.requestLocalClose();
      connectionState.markLocalFinished();
      reply = HandshakeReply.FIN_ACK;
    }
    return new ReceiveDecision(false, reply);
  }

  private static boolean handlesInboundType(DpchPacketType packetType) {
    return packetType == DpchPacketType.DPCH_PACKET_TYPE_SYN || packetType == DpchPacketType.DPCH_PACKET_TYPE_FIN || packetType == DpchPacketType.DPCH_PACKET_TYPE_DATA;
  }

  @Override
  public void close() throws Exception {
    if (!context.stop()) {
      return;
    }
    try {
      context.shutdown();
      workerThread.interrupt();
      context.perfectLink.close();
    } finally {
      LinkThreadUtil.awaitStop(workerThread, "handshaked-perfect-link");
    }
  }
}
