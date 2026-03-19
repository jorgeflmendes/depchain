package pt.ulisboa.depchain.shared.network.links.perfect;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.InvalidProtocolBufferException;

import pt.ulisboa.depchain.proto.DpchPacket;
import pt.ulisboa.depchain.proto.DpchPacketType;
import pt.ulisboa.depchain.shared.network.links.AsyncLinkContext;
import pt.ulisboa.depchain.shared.network.links.LinkFailureException;
import pt.ulisboa.depchain.shared.network.links.fairloss.InboundBytes;
import pt.ulisboa.depchain.shared.network.links.stubborn.StubbornLink;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedKey;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.network.packet.DpchPacketUtil;
import pt.ulisboa.depchain.shared.utils.ProtoValidationUtil;
import pt.ulisboa.depchain.shared.utils.TimeUtil;
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

  void releaseConnection(long connectionId, InetSocketAddress remoteEndpoint) {
    ValidationUtils.requireNonNull(remoteEndpoint, "remoteEndpoint");
    ConnectionKey key = new ConnectionKey(remoteEndpoint, connectionId);
    PerfectConnectionState connectionState = connectionStates.remove(key);
    if (connectionState != null) {
      cancelTrackedBatch(connectionState.senderState().takeAllTracked(connectionId), remoteEndpoint);
    }
  }

  boolean awaitNoPendingType(long connectionId, InetSocketAddress remoteEndpoint, DpchPacketType packetType, long timeoutMs) throws InterruptedException {
    ValidationUtils.requireAllNonNull(named("remoteEndpoint", remoteEndpoint), named("packetType", packetType));
    if (packetType == DpchPacketType.DPCH_PACKET_TYPE_ACK) {
      throw new IllegalArgumentException("ACK packets are not tracked");
    }

    long checkedTimeoutMs = ValidationUtils.requireNonNegativeLong(timeoutMs, "timeoutMs");
    long nowMs = TimeUtil.nowMs();
    long deadlineMs = Long.MAX_VALUE;
    if (checkedTimeoutMs < Long.MAX_VALUE - nowMs) {
      deadlineMs = TimeUtil.deadlineAfter(nowMs, checkedTimeoutMs);
    }
    if (!isRunning()) {
      return false;
    }

    PerfectConnectionState connectionState = connectionStates.get(new ConnectionKey(remoteEndpoint, connectionId));
    return connectionState == null || connectionState.senderState().waitUntilNoPending(this, connectionId, remoteEndpoint, packetType, deadlineMs);
  }

  LinkFailureException pollTerminalFailure(long connectionId, InetSocketAddress remoteEndpoint, int sequenceNumber, DpchPacketType packetType) {
    ValidationUtils.requireAllNonNull(named("remoteEndpoint", remoteEndpoint), named("packetType", packetType));
    TrackedKey trackedKey = new TrackedKey(connectionId, sequenceNumber, packetType.getNumber());
    return stubbornLink.pollTerminalFailure(trackedKey, remoteEndpoint);
  }

  void cancelTrackedBatch(Iterable<TrackedKey> trackedKeys, InetSocketAddress remoteEndpoint) {
    ValidationUtils.requireAllNonNull(named("trackedKeys", trackedKeys), named("remoteEndpoint", remoteEndpoint));
    for (TrackedKey trackedKey : trackedKeys) {
      stubbornLink.cancelTracked(trackedKey, remoteEndpoint);
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
