package pt.ulisboa.depchain.shared.network.links.perfect;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.InvalidProtocolBufferException;

import pt.ulisboa.depchain.proto.DpchPacket;
import pt.ulisboa.depchain.proto.DpchPacketType;
import pt.ulisboa.depchain.shared.network.links.AsyncLinkContext;
import pt.ulisboa.depchain.shared.network.links.fairloss.InboundBytes;
import pt.ulisboa.depchain.shared.network.links.stubborn.StubbornLink;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.network.packet.DpchPacketUtil;
import pt.ulisboa.depchain.shared.utils.ProtoValidationUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class PerfectContext extends AsyncLinkContext<InboundPacket> {
  static final byte[] ACK_DATA = new byte[]{DpchPacketUtil.encodeReliableType(DpchPacketType.DPCH_PACKET_TYPE_DATA)};
  static final byte[] ACK_SYN = new byte[]{DpchPacketUtil.encodeReliableType(DpchPacketType.DPCH_PACKET_TYPE_SYN)};
  static final byte[] ACK_FIN = new byte[]{DpchPacketUtil.encodeReliableType(DpchPacketType.DPCH_PACKET_TYPE_FIN)};

  final StubbornLink stubbornLink;

  final Map<ConnectionKey, PerfectConnectionState> connectionStates = new ConcurrentHashMap<>();

  PerfectContext(StubbornLink stubbornLink) {
    this.stubbornLink = ValidationUtils.requireNonNull(stubbornLink, "stubbornLink");
  }

  void shutdown() {
    shutdownInbox();
    for (PerfectConnectionState connectionState : connectionStates.values()) {
      connectionState.senderState().notifyWaiters();
    }
  }

  static byte[] ackPayloadFor(DpchPacketType type) {
    return switch (type) {
      case DPCH_PACKET_TYPE_DATA -> ACK_DATA;
      case DPCH_PACKET_TYPE_SYN -> ACK_SYN;
      case DPCH_PACKET_TYPE_FIN -> ACK_FIN;
      case DPCH_PACKET_TYPE_ACK, DPCH_PACKET_TYPE_UNSPECIFIED, UNRECOGNIZED -> throw new IllegalArgumentException("ACK cannot acknowledge " + type);
    };
  }

  static DpchPacketType decodeAcknowledgedType(byte[] ackPayload, DpchPacketType fallback) {
    if (ackPayload.length == 0) {
      return fallback;
    }
    if (ackPayload.length != 1) {
      return null;
    }

    try {
      return DpchPacketUtil.decodeReliableType(ackPayload[0]);
    } catch (IllegalArgumentException ignored) {
      return null;
    }
  }

  static byte[] serializePacket(DpchPacket packet) {
    return ProtoValidationUtil.requireValid(packet, "DpchPacket").toByteArray();
  }

  static InboundPacket decodeInboundPacket(InboundBytes datagram) throws IOException {
    if (datagram == null) {
      return null;
    }
    try {
      DpchPacket packet = ProtoValidationUtil.requireValid(DpchPacket.parseFrom(datagram.payload()), "DpchPacket");
      return new InboundPacket(datagram.sender(), packet);
    } catch (InvalidProtocolBufferException | IllegalArgumentException exception) {
      throw new IOException("Invalid protobuf DPCH packet", exception);
    }
  }
}
