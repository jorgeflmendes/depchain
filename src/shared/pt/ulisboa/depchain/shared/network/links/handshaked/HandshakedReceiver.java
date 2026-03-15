package pt.ulisboa.depchain.shared.network.links.handshaked;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.net.InetSocketAddress;

import pt.ulisboa.depchain.proto.DpchPacketType;
import pt.ulisboa.depchain.shared.logging.Logger;
import pt.ulisboa.depchain.shared.network.packet.DpchPacketUtil;
import pt.ulisboa.depchain.shared.network.links.LinkClosedException;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class HandshakedReceiver {
  private static final Logger logger = new Logger("HandshakedReceiver");
  private record ReceiveResult(boolean deliver, HandshakeReply reply) {
  }

  private final HandshakedContext context;
  private final HandshakedSender sender;

  HandshakedReceiver(HandshakedContext context, HandshakedSender sender) {
    ValidationUtils.requireAllNonNull(named("context", context), named("sender", sender));
    this.context = context;
    this.sender = sender;
  }

  void runInboundLoop() {
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
        logger.debug("HandshakedPerfectLink worker error: " + exception.getMessage());
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
    if (inbound == null) {
      return null;
    }

    DpchPacketType packetType = inbound.packet().getPacketType();
    if (!handlesInboundType(packetType)) {
      return null;
    }

    long connectionId = inbound.packet().getConnectionId();
    int sequenceNumber = inbound.packet().getSequenceNumber();
    InetSocketAddress remote = inbound.sender();

    ConnectionKey connectionKey = new ConnectionKey(remote, connectionId);
    ConnectionState connectionState = context.connectionStateRegistry.getOrCreate(connectionKey);
    ReceiveResult result;
    synchronized (connectionState) {
      result = decidePacket(connectionState, packetType, DpchPacketUtil.hasType(inbound.packet(), DpchPacketType.DPCH_PACKET_TYPE_ACK));
    }

    sender.sendHandshakeReply(result.reply(), connectionId, sequenceNumber, packetType, remote);
    if (packetType == DpchPacketType.DPCH_PACKET_TYPE_FIN) {
      // Stop retrying old packets once the peer started closing this connection.
      context.perfectLink.cancelPendingData(connectionId, remote);
      context.perfectLink.cancelPendingControl(connectionId, remote);
    }
    if (packetType == DpchPacketType.DPCH_PACKET_TYPE_FIN) {
      synchronized (connectionState) {
        if (connectionState.isCloseConverged()) {
          context.perfectLink.releaseConnection(connectionId, remote);
        }
      }
    }
    if (result.deliver()) {
      return inbound;
    }
    return null;
  }

  private static ReceiveResult decidePacket(ConnectionState state, DpchPacketType type, boolean hasAck) {
    if (type == DpchPacketType.DPCH_PACKET_TYPE_SYN) {
      return decideSyn(state, hasAck);
    }
    if (type == DpchPacketType.DPCH_PACKET_TYPE_FIN) {
      return decideFin(state);
    }
    return decideData(state);
  }

  private static ReceiveResult decideSyn(ConnectionState state, boolean hasAck) {
    state.markRemoteEstablishedIfNotFinished();
    if (state.isClosing()) {
      return new ReceiveResult(false, HandshakeReply.ACK);
    }

    if (!hasAck) {
      if (state.shouldSendSyn()) {
        state.markLocalEstablished();
      }
      return new ReceiveResult(false, HandshakeReply.SYN_ACK);
    }

    HandshakeReply reply = HandshakeReply.ACK;
    if (state.shouldSendSyn()) {
      state.markLocalEstablished();
      reply = HandshakeReply.SYN_ACK;
    }

    return new ReceiveResult(false, reply);
  }

  private static ReceiveResult decideFin(ConnectionState state) {
    state.markRemoteFinished();
    HandshakeReply reply = HandshakeReply.ACK;

    if (!state.isLocalFinished()) {
      // Remote FIN means this connection cannot exchange more data, so close locally too.
      state.requestLocalClose();
      state.markLocalFinished();
      reply = HandshakeReply.FIN_ACK;
    }

    return new ReceiveResult(false, reply);
  }

  private static ReceiveResult decideData(ConnectionState state) {
    return new ReceiveResult(state.canExchangeData(), HandshakeReply.NONE);
  }

  private static boolean handlesInboundType(DpchPacketType type) {
    return type == DpchPacketType.DPCH_PACKET_TYPE_SYN || type == DpchPacketType.DPCH_PACKET_TYPE_FIN || type == DpchPacketType.DPCH_PACKET_TYPE_DATA;
  }
}

