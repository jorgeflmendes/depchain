package pt.ulisboa.depchain.shared.network.links.handshaked;

import pt.ulisboa.depchain.shared.network.dpch.DpchType;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

// Decides how to handle inbound messages based on their type and the current connection state
final class InboundHandshakeDecider {
  // Control reply types for handshake messages
  enum ControlReply {
    NONE,
    ACK,
    SYN_ACK,
    FIN_ACK
  }

  // Represents the decision on whether to deliver data and what control reply to send for an inbound message
  record InboundDecision(boolean deliverData, ControlReply reply) {
    InboundDecision {
      ValidationUtils.requireNonNull(reply, "reply");
    }
  }

  // Determines whether to deliver data and what control reply to send based on the message type and connection state
  static InboundDecision decideInboundLocked(ConnectionState state, DpchType type, long now) {
    state.touch(now);

    if (type == DpchType.SYN) {
      state.markRemoteEstablishedIfNotFinished();
      ControlReply reply = ControlReply.ACK;
      if (state.shouldSendSyn() && !state.isClosing()) {
        state.markLocalEstablished();
        reply = ControlReply.SYN_ACK;
      }
      return new InboundDecision(false, reply);
    }

    if (type == DpchType.FIN) {
      state.markRemoteFinished();
      ControlReply reply = ControlReply.ACK;

      if (state.isLocalCloseRequested() && !state.isLocalFinished()) {
        state.markLocalFinished();
        reply = ControlReply.FIN_ACK;
      }
      return new InboundDecision(false, reply);
    }

    return new InboundDecision(state.canExchangeData(), ControlReply.NONE);
  }

  static boolean handlesInboundType(DpchType type) {
    return type == DpchType.SYN || type == DpchType.FIN || type == DpchType.DATA;
  }
}
