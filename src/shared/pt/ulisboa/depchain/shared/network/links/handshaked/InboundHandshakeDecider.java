package pt.ulisboa.depchain.shared.network.links.handshaked;

import pt.ulisboa.depchain.shared.network.dpch.DpchType;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

// Decides how to handle inbound messages based on their type and the current connection state
final class InboundHandshakeDecider {
  // Handshake/control replies emitted by this decider for SYN/FIN processing.
  enum HandshakeReply {
    NONE,
    ACK,
    SYN_ACK,
    FIN_ACK
  }

  // Represents the decision on whether to deliver data and what control reply to send for an inbound message
  record InboundDecision(boolean deliverData, HandshakeReply reply) {
    InboundDecision {
      ValidationUtils.requireNonNull(reply, "reply");
    }
  }

  // Entry point: routes inbound packet to the type-specific decision path.
  // "Locked" means caller must already hold the monitor for the given ConnectionState.
  static InboundDecision decideInboundLocked(ConnectionState state, DpchType type, boolean inboundHasAck, long now) {
    state.touch(now);
    if (type == DpchType.SYN) {
      return decideSynLocked(state, inboundHasAck);
    }
    if (type == DpchType.FIN) {
      return decideFinLocked(state);
    }
    return decideDataLocked(state);
  }

  // SYN-specific branch of the state machine (called while the connection state lock is held).
  private static InboundDecision decideSynLocked(ConnectionState state, boolean inboundHasAck) {
    state.markRemoteEstablishedIfNotFinished();
    // Closing state has priority: do not reopen a stream while close is in progress.
    if (state.isClosing()) {
      return new InboundDecision(false, HandshakeReply.ACK);
    }

    // Pure SYN (no ACK bit) is treated as an open request and always answered with SYN|ACK (TCP-like behaviour when the first SYN/ACK is lost).
    if (!inboundHasAck) {
      if (state.shouldSendSyn()) {
        state.markLocalEstablished();
      }
      return new InboundDecision(false, HandshakeReply.SYN_ACK);
    }

    // SYN carrying ACK is interpreted as a control retry/combined control message (keep ACK-only reply).
    HandshakeReply reply = HandshakeReply.ACK;
    if (state.shouldSendSyn()) {
      state.markLocalEstablished();
      reply = HandshakeReply.SYN_ACK;
    }
    return new InboundDecision(false, reply);
  }

  // FIN-specific branch of the state machine (called while the connection state lock is held).
  private static InboundDecision decideFinLocked(ConnectionState state) {
    state.markRemoteFinished();
    HandshakeReply reply = HandshakeReply.ACK;

    if (state.isLocalCloseRequested() && !state.isLocalFinished()) {
      state.markLocalFinished();
      reply = HandshakeReply.FIN_ACK;
    }
    return new InboundDecision(false, reply);
  }

  // DATA-specific branch: only decides delivery gating, not ACK emission (called while lock is held).
  private static InboundDecision decideDataLocked(ConnectionState state) {
    // Handshaked layer only gates DATA delivery; DATA acknowledgments are emitted by PerfectLink.
    return new InboundDecision(state.canExchangeData(), HandshakeReply.NONE);
  }

  static boolean handlesInboundType(DpchType type) {
    return type == DpchType.SYN || type == DpchType.FIN || type == DpchType.DATA;
  }
}
