package pt.ulisboa.depchain.shared.network.links.perfect;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.dpch.DpchSerialization;
import pt.ulisboa.depchain.shared.network.dpch.DpchType;
import pt.ulisboa.depchain.shared.network.links.fairloss.InboundBytes;
import pt.ulisboa.depchain.shared.network.links.stubborn.StubbornLink;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.TimeUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class PerfectContext {
  static final byte[] ACK_DATA = new byte[]{DpchType.DATA.code()};
  static final byte[] ACK_SYN = new byte[]{DpchType.SYN.code()};
  static final byte[] ACK_FIN = new byte[]{DpchType.FIN.code()};

  final StubbornLink stubbornLink;
  final int maxWindowSize;
  final long streamIdleTtlMs;
  final long cleanupCheckIntervalMs;
  volatile long nextCleanupAtMs;

  final Map<ConnectionKey, PerfectSender.SenderState> sendSequences = new ConcurrentHashMap<>();
  final Map<ConnectionKey, PerfectReceiver.ReceiverState> receiverStates = new ConcurrentHashMap<>();
  final Map<ConnectionKey, Integer> deliveredSequenceFloors = new ConcurrentHashMap<>();
  final BlockingQueue<InboundPacket> deliveryQueue = new LinkedBlockingQueue<>();
  final AtomicBoolean running = new AtomicBoolean(true);

  PerfectContext(StubbornLink stubbornLink, int maxWindowSize, long streamIdleTtlMs) {
    this.stubbornLink = ValidationUtils.requireNonNull(stubbornLink, "stubbornLink");
    this.maxWindowSize = ValidationUtils.requirePositiveInt(maxWindowSize, "maxWindowSize");
    this.streamIdleTtlMs = ValidationUtils.requirePositiveLong(streamIdleTtlMs, "streamIdleTtlMs");
    this.cleanupCheckIntervalMs = Math.max(250L, Math.min(this.streamIdleTtlMs, 5_000L));
    this.nextCleanupAtMs = TimeUtil.deadlineAfter(cleanupCheckIntervalMs);
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

    long deadlineMs = TimeUtil.deadlineAfter(ValidationUtils.requireNonNegativeLong(timeoutMs, "timeoutMs"));
    if (!running.get()) {
      return false;
    }

    PerfectSender.SenderState senderState = sendSequences.get(new ConnectionKey(remoteEndpoint, connectionId));
    return senderState == null || senderState.waitUntilNoPending(packetType, deadlineMs);
  }

  long trackedNoPendingTimeoutMs() {
    long retransmitBudgetMs = stubbornLink.baseDelayMs() + stubbornLink.maxDelayMs();
    long trackedLifetimeMs = stubbornLink.maxTrackedLifetimeMs();

    long boundedByLifetime;
    if (trackedLifetimeMs < 0L) {
      boundedByLifetime = retransmitBudgetMs;
    } else {
      boundedByLifetime = Math.min(retransmitBudgetMs, trackedLifetimeMs);
    }

    return Math.max(1L, Math.min(streamIdleTtlMs, boundedByLifetime));
  }

  void cleanStaleStatesIfNeeded(long now) {
    if (!TimeUtil.hasReachedDeadline(now, nextCleanupAtMs)) {
      return;
    }

    nextCleanupAtMs = TimeUtil.deadlineAfter(now, cleanupCheckIntervalMs);

    // Re-check staleness under the state lock before removing sender state.
    for (ConnectionKey connectionKey : new ArrayList<>(sendSequences.keySet())) {
      sendSequences.computeIfPresent(connectionKey, (ignored, state) -> {
        synchronized (state) {
          if (!state.isStale(now, streamIdleTtlMs) || !state.hasNoPendingMessages()) {
            return state;
          }

          state.notifyAll();
          return null;
        }
      });
    }

    // Persist the delivery floor before removing receiver state.
    for (ConnectionKey connectionKey : new ArrayList<>(receiverStates.keySet())) {
      receiverStates.computeIfPresent(connectionKey, (ignored, state) -> {
        synchronized (state) {
          if (!state.isStale(now, streamIdleTtlMs)) {
            return state;
          }

          deliveredSequenceFloors.merge(connectionKey, state.nextExpectedSequence(), Math::max);
          return null;
        }
      });
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
