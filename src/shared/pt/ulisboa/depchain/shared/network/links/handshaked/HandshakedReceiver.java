package pt.ulisboa.depchain.shared.network.links.handshaked;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.net.InetSocketAddress;

import pt.ulisboa.depchain.shared.network.dpch.DpchType;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class HandshakedReceiver {
  private record InboundDecision(boolean deliverData, HandshakeReply reply) {
  }

  private final HandshakedContext context;
  private final HandshakedSender sender;

  HandshakedReceiver(HandshakedContext context, HandshakedSender sender) {
    ValidationUtils.requireAllNonNull(named("context", context), named("sender", sender));
    this.context = context;
    this.sender = sender;
  }

  void runInboundLoop() {
    while (context.running.get()) {
      try {
        InboundPacket delivered = handleInbound(context.perfectLink.receive());
        if (delivered != null) {
          context.deliveryQueue.offer(delivered);
        }
      } catch (InterruptedException interrupted) {
        if (!context.running.get()) {
          break;
        }
        Thread.currentThread().interrupt();
        break;
      } catch (RuntimeException exception) {
        if (!context.running.get()) {
          break;
        }
        System.err.println("HandshakedPerfectLink worker error: " + exception.getMessage());
      }
    }
  }

  private InboundPacket handleInbound(InboundPacket inbound) {
    DpchType packetType = inbound.packet().type();
    if (!handlesInboundType(packetType)) {
      return null;
    }

    long connectionId = inbound.packet().connectionId();
    int sequenceNumber = inbound.packet().sequenceNumber();
    InetSocketAddress remote = inbound.sender();

    ConnectionKey connectionKey = new ConnectionKey(remote, connectionId);
    long now = System.currentTimeMillis();
    if (context.closedConnectionsRegistry.isClosedRecently(connectionKey, now)) {
      if (packetType == DpchType.SYN || packetType == DpchType.FIN) {
        sender.sendControlReply(HandshakeReply.ACK, connectionId, sequenceNumber, packetType, remote);
      }
      return null;
    }

    ConnectionState connectionState = context.connectionStateRegistry.getOrCreate(connectionKey);
    // Mark the state as active so cleanup cannot remove it mid-receive.
    markConnectionStateInUse(connectionState);
    try {
      InboundDecision decision;
      synchronized (connectionState) {
        decision = decideInboundLocked(connectionState, packetType, inbound.packet().hasType(DpchType.ACK), now);
      }

      sender.sendControlReply(decision.reply(), connectionId, sequenceNumber, packetType, remote);
      if (packetType == DpchType.SYN || packetType == DpchType.FIN) {
        synchronized (connectionState) {
          connectionState.notifyAll();
        }
      }
      context.connectionStateRegistry.cleanup(System.currentTimeMillis(), context.closedConnectionsRegistry);
      if (decision.deliverData()) {
        return inbound;
      }
      return null;
    } finally {
      unmarkConnectionStateInUse(connectionState);
    }
  }

  private static InboundDecision decideInboundLocked(ConnectionState state, DpchType type, boolean inboundHasAck, long now) {
    state.touch(now);
    if (type == DpchType.SYN) {
      return decideSynLocked(state, inboundHasAck);
    }
    if (type == DpchType.FIN) {
      return decideFinLocked(state);
    }
    return decideDataLocked(state);
  }

  private static InboundDecision decideSynLocked(ConnectionState state, boolean inboundHasAck) {
    state.markRemoteEstablishedIfNotFinished();
    if (state.isClosing()) {
      return new InboundDecision(false, HandshakeReply.ACK);
    }

    if (!inboundHasAck) {
      if (state.shouldSendSyn()) {
        state.markLocalEstablished();
      }
      return new InboundDecision(false, HandshakeReply.SYN_ACK);
    }

    HandshakeReply reply = HandshakeReply.ACK;
    if (state.shouldSendSyn()) {
      state.markLocalEstablished();
      reply = HandshakeReply.SYN_ACK;
    }

    return new InboundDecision(false, reply);
  }

  private static InboundDecision decideFinLocked(ConnectionState state) {
    state.markRemoteFinished();
    HandshakeReply reply = HandshakeReply.ACK;

    if (state.isLocalCloseRequested() && !state.isLocalFinished()) {
      state.markLocalFinished();
      reply = HandshakeReply.FIN_ACK;
    }

    return new InboundDecision(false, reply);
  }

  private static InboundDecision decideDataLocked(ConnectionState state) {
    return new InboundDecision(state.canExchangeData(), HandshakeReply.NONE);
  }

  private static boolean handlesInboundType(DpchType type) {
    return type == DpchType.SYN || type == DpchType.FIN || type == DpchType.DATA;
  }

  private static void markConnectionStateInUse(ConnectionState connectionState) {
    synchronized (connectionState) {
      connectionState.markInUse();
    }
  }

  private static void unmarkConnectionStateInUse(ConnectionState connectionState) {
    synchronized (connectionState) {
      connectionState.unmarkInUse();
      connectionState.notifyAll();
    }
  }
}
