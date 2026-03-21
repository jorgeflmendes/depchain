package pt.ulisboa.depchain.shared.network.links.perfect;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.protobuf.InvalidProtocolBufferException;

import pt.ulisboa.depchain.proto.DpchPacket;
import pt.ulisboa.depchain.proto.DpchPacketType;
import pt.ulisboa.depchain.shared.network.links.fairloss.InboundBytes;
import pt.ulisboa.depchain.shared.network.links.stubborn.StubbornLink;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class PerfectContext {
  final StubbornLink stubbornLink;
  final Map<ConnectionKey, PerfectConnectionState> connectionStates = new ConcurrentHashMap<>();
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final Deque<InboundPacket> readyPackets = new ArrayDeque<>();

  PerfectContext(StubbornLink stubbornLink) {
    this.stubbornLink = ValidationUtils.requireNonNull(stubbornLink, "stubbornLink");
  }

  boolean isRunning() {
    return running.get();
  }

  boolean stop() {
    return running.compareAndSet(true, false);
  }

  synchronized InboundPacket pollDelivered() {
    return readyPackets.pollFirst();
  }

  synchronized void offerDelivered(InboundPacket packet) {
    readyPackets.addLast(ValidationUtils.requireNonNull(packet, "packet"));
  }

  void shutdown() {
    for (PerfectConnectionState connectionState : connectionStates.values()) {
      connectionState.senderState().notifyWaiters();
    }
  }

  PerfectConnectionState getOrCreateState(ConnectionKey connectionKey) {
    PerfectConnectionState state = connectionStates.get(connectionKey);
    if (state != null) {
      return state;
    }

    PerfectConnectionState newState = new PerfectConnectionState();
    PerfectConnectionState racedState = connectionStates.putIfAbsent(connectionKey, newState);
    if (racedState != null) {
      return racedState;
    }
    return newState;
  }

  static byte[] encodePacket(DpchPacket packet) {
    return packet.toByteArray();
  }

  static InboundPacket parseInboundPacket(InboundBytes datagram) throws IOException {
    if (datagram == null) {
      return null;
    }

    try {
      DpchPacket packet = DpchPacket.parseFrom(ByteBuffer.wrap(datagram.payloadView(), 0, datagram.payloadLength()));
      return new InboundPacket(datagram.sender(), packet);
    } catch (InvalidProtocolBufferException | IllegalArgumentException exception) {
      throw new IOException("Invalid protobuf DPCH packet", exception);
    }
  }

  static DpchPacket buildDataPacket(long connectionId, int sequenceNumber, byte[] payload) {
    return DpchPacket.newBuilder().setConnectionId(connectionId).setPacketType(DpchPacketType.DPCH_PACKET_TYPE_DATA).setSequenceNumber(sequenceNumber)
        .setPayload(com.google.protobuf.ByteString.copyFrom(payload)).build();
  }

  static DpchPacket buildAckPacket(long connectionId, int sequenceNumber) {
    return DpchPacket.newBuilder().setConnectionId(connectionId).setPacketType(DpchPacketType.DPCH_PACKET_TYPE_ACK).setSequenceNumber(sequenceNumber).build();
  }
}
