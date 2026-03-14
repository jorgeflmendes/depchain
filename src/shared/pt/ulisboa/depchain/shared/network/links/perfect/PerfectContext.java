package pt.ulisboa.depchain.shared.network.links.perfect;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import pt.ulisboa.depchain.shared.network.packet.DpchPacket;
import pt.ulisboa.depchain.shared.network.packet.DpchSerialization;
import pt.ulisboa.depchain.shared.network.packet.DpchType;
import pt.ulisboa.depchain.shared.network.links.AsyncLinkContext;
import pt.ulisboa.depchain.shared.network.links.LinkFailureException;
import pt.ulisboa.depchain.shared.network.links.fairloss.InboundBytes;
import pt.ulisboa.depchain.shared.network.links.stubborn.StubbornLink;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedKey;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.TimeUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class PerfectContext extends AsyncLinkContext<InboundPacket> {
  static final byte[] ACK_DATA = new byte[]{DpchType.DATA.code()};
  static final byte[] ACK_SYN = new byte[]{DpchType.SYN.code()};
  static final byte[] ACK_FIN = new byte[]{DpchType.FIN.code()};

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

  boolean awaitNoPendingType(long connectionId, InetSocketAddress remoteEndpoint, DpchType packetType, long timeoutMs) throws InterruptedException {
    ValidationUtils.requireAllNonNull(named("remoteEndpoint", remoteEndpoint), named("packetType", packetType));
    if (packetType == DpchType.ACK) {
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

  LinkFailureException pollTerminalFailure(long connectionId, InetSocketAddress remoteEndpoint, int sequenceNumber, DpchType packetType) {
    ValidationUtils.requireAllNonNull(named("remoteEndpoint", remoteEndpoint), named("packetType", packetType));
    TrackedKey trackedKey = new TrackedKey(connectionId, sequenceNumber, Byte.toUnsignedInt(packetType.code()));
    return stubbornLink.pollTerminalFailure(trackedKey, remoteEndpoint);
  }

  void cancelTrackedBatch(Iterable<TrackedKey> trackedKeys, InetSocketAddress remoteEndpoint) {
    ValidationUtils.requireAllNonNull(named("trackedKeys", trackedKeys), named("remoteEndpoint", remoteEndpoint));
    for (TrackedKey trackedKey : trackedKeys) {
      stubbornLink.cancelTracked(trackedKey, remoteEndpoint);
    }
  }

  static byte[] ackPayloadFor(DpchType type) {
    return switch (type) {
      case DATA -> ACK_DATA;
      case SYN -> ACK_SYN;
      case FIN -> ACK_FIN;
      case ACK -> throw new IllegalArgumentException("ACK cannot acknowledge ACK");
    };
  }

  static DpchType decodeAcknowledgedType(byte[] ackPayload, DpchType fallback) {
    if (ackPayload.length == 0) {
      return fallback;
    }
    if (ackPayload.length != 1) {
      return null;
    }

    try {
      DpchType decoded = DpchType.fromCode(ackPayload[0]);

      if (decoded == DpchType.DATA || decoded == DpchType.SYN || decoded == DpchType.FIN) {
        return decoded;
      }

      return null;
    } catch (IllegalArgumentException ignored) {
      return null;
    }
  }

  static byte[] serializePacket(DpchPacket packet) {
    try {
      return DpchSerialization.toBytes(packet);
    } catch (IOException exception) {
      throw new IllegalStateException("Failed to serialize DPCH packet", exception);
    }
  }

  static InboundPacket decodeInboundPacket(InboundBytes datagram) throws IOException {
    if (datagram == null) {
      return null;
    }
    byte[] payload = datagram.payload();
    DpchPacket packet = DpchSerialization.fromBytes(payload, 0, payload.length);
    return new InboundPacket(datagram.sender(), packet);
  }
}

