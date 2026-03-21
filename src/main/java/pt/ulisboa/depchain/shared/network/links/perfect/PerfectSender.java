package pt.ulisboa.depchain.shared.network.links.perfect;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.ulisboa.depchain.proto.DpchPacket;
import pt.ulisboa.depchain.proto.DpchPacketType;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedKey;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.utils.TimeUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class PerfectSender {
  private static final Logger logger = LoggerFactory.getLogger(PerfectSender.class);
  private final PerfectContext context;

  PerfectSender(PerfectContext context) {
    this.context = ValidationUtils.requireNonNull(context, "context");
  }

  void sendData(long connectionId, byte[] payload, InetSocketAddress remoteEndpoint) {
    ValidationUtils.requireNonNull(payload, "payload");
    ValidationUtils.requireNonNull(remoteEndpoint, "remoteEndpoint");

    ConnectionKey connectionKey = new ConnectionKey(remoteEndpoint, connectionId);
    SenderState senderState = context.getOrCreateState(connectionKey).senderState();
    int sequenceNumber = senderState.nextSequence();
    DpchPacket packet = PerfectContext.buildDataPacket(connectionId, sequenceNumber, payload);
    context.stubbornLink.sendTrackedWithTerminalNotification(new TrackedKey(connectionId, sequenceNumber, DpchPacketType.DPCH_PACKET_TYPE_DATA.getNumber()), PerfectContext
        .encodePacket(packet), remoteEndpoint, senderState.terminalNotifier());
  }

  void sendAck(long connectionId, int acknowledgedSequence, InetSocketAddress remoteEndpoint) {
    if (!context.isRunning()) {
      return;
    }

    try {
      context.stubbornLink.send(PerfectContext.encodePacket(PerfectContext.buildAckPacket(connectionId, acknowledgedSequence)), remoteEndpoint);
    } catch (IOException | RuntimeException exception) {
      if (!context.isRunning()) {
        return;
      }
      logger.debug("Failed to send ACK for connection {} seq={}", connectionId, acknowledgedSequence, exception);
    }
  }

  boolean awaitNoPendingData(long connectionId, InetSocketAddress remoteEndpoint, long timeoutMs) throws InterruptedException {
    ValidationUtils.requireNonNull(remoteEndpoint, "remoteEndpoint");
    long checkedTimeoutMs = ValidationUtils.requireNonNegativeLong(timeoutMs, "timeoutMs");
    long nowMs = TimeUtil.nowMs();
    long deadlineMs = Long.MAX_VALUE;
    if (checkedTimeoutMs < Long.MAX_VALUE - nowMs) {
      deadlineMs = TimeUtil.deadlineAfter(nowMs, checkedTimeoutMs);
    }
    if (!context.isRunning()) {
      return false;
    }

    PerfectConnectionState connectionState = context.connectionStates.get(new ConnectionKey(remoteEndpoint, connectionId));
    return connectionState == null || connectionState.senderState().waitUntilNoPendingData(context, connectionId, remoteEndpoint, deadlineMs);
  }

  void cancelPendingData(long connectionId, InetSocketAddress remoteEndpoint) {
    ConnectionKey key = new ConnectionKey(remoteEndpoint, connectionId);
    PerfectConnectionState connectionState = context.connectionStates.get(key);
    if (connectionState == null) {
      return;
    }

    List<TrackedKey> cancellations = connectionState.senderState().cancelPendingData(connectionId);
    for (TrackedKey trackedKey : cancellations) {
      context.stubbornLink.cancelTracked(trackedKey, remoteEndpoint);
    }
  }
}
