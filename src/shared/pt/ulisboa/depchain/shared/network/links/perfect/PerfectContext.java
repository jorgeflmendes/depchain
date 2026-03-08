package pt.ulisboa.depchain.shared.network.links.perfect;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.dpch.DpchSerialization;
import pt.ulisboa.depchain.shared.network.dpch.DpchType;
import pt.ulisboa.depchain.shared.network.links.LinkFailureException;
import pt.ulisboa.depchain.shared.network.links.fairloss.InboundBytes;
import pt.ulisboa.depchain.shared.network.links.stubborn.StubbornLink;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedKey;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.TimeUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class PerfectContext {
  static final byte[] ACK_DATA = new byte[]{DpchType.DATA.code()};
  static final byte[] ACK_SYN = new byte[]{DpchType.SYN.code()};
  static final byte[] ACK_FIN = new byte[]{DpchType.FIN.code()};

  final StubbornLink stubbornLink;

  final Map<ConnectionKey, PerfectSender.SenderState> sendSequences = new ConcurrentHashMap<>();
  final Map<ConnectionKey, PerfectReceiver.ReceiverState> receiverStates = new ConcurrentHashMap<>();
  final BlockingQueue<InboundPacket> deliveryQueue = new LinkedBlockingQueue<>();
  final AtomicBoolean running = new AtomicBoolean(true);

  PerfectContext(StubbornLink stubbornLink) {
    this.stubbornLink = ValidationUtils.requireNonNull(stubbornLink, "stubbornLink");
  }

  InboundPacket receive() throws InterruptedException {
    return deliveryQueue.take();
  }

  InboundPacket receive(long timeoutMs) throws InterruptedException {
    return deliveryQueue.poll(ValidationUtils.requireNonNegativeLong(timeoutMs, "timeoutMs"), TimeUnit.MILLISECONDS);
  }

  boolean waitUntilNoPendingType(long connectionId, InetSocketAddress remoteEndpoint, DpchType packetType, long timeoutMs) throws InterruptedException {
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
    if (!running.get()) {
      return false;
    }

    PerfectSender.SenderState senderState = sendSequences.get(new ConnectionKey(remoteEndpoint, connectionId));
    return senderState == null || senderState.waitUntilNoPending(this, connectionId, remoteEndpoint, packetType, deadlineMs);
  }

  LinkFailureException trackedFailureOrNull(long connectionId, InetSocketAddress remoteEndpoint, int sequenceNumber, DpchType packetType) {
    ValidationUtils.requireAllNonNull(named("remoteEndpoint", remoteEndpoint), named("packetType", packetType));
    TrackedKey trackedKey = new TrackedKey(connectionId, sequenceNumber, Byte.toUnsignedInt(packetType.code()));
    return stubbornLink.trackedFailureOrNull(trackedKey, remoteEndpoint);
  }

  void throwIfTrackedFailed(long connectionId, InetSocketAddress remoteEndpoint, DpchType packetType) {
    ValidationUtils.requireAllNonNull(named("remoteEndpoint", remoteEndpoint), named("packetType", packetType));
    PerfectSender.SenderState senderState = sendSequences.get(new ConnectionKey(remoteEndpoint, connectionId));
    if (senderState == null) {
      return;
    }

    LinkFailureException failure = senderState.failureForTypeOrNull(this, connectionId, remoteEndpoint, packetType);
    if (failure != null) {
      throw failure;
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

  static byte[] serializePacket(Dpch packet) {
    try {
      return DpchSerialization.toBytes(packet);
    } catch (IOException exception) {
      throw new IllegalStateException("Failed to serialize DPCH packet", exception);
    }
  }

  static InboundPacket decodeInboundPacket(InboundBytes datagram) throws IOException {
    byte[] payload = datagram.payload();
    Dpch packet = DpchSerialization.fromBytes(payload, 0, payload.length);
    return new InboundPacket(datagram.sender(), packet);
  }
}
