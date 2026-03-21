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
import pt.ulisboa.depchain.shared.utils.ProtoValidationUtil;
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

  synchronized InboundPacket pollReady() {
    return readyPackets.pollFirst();
  }

  synchronized void offerReady(InboundPacket packet) {
    readyPackets.addLast(ValidationUtils.requireNonNull(packet, "packet"));
  }

  void shutdown() {
    for (PerfectConnectionState connectionState : connectionStates.values()) {
      connectionState.senderState().notifyWaiters();
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
      DpchPacket packet = ProtoValidationUtil.requireValid(DpchPacket.parseFrom(ByteBuffer.wrap(datagram.payloadView(), 0, datagram.payloadLength())), "DpchPacket");
      return new InboundPacket(datagram.sender(), packet);
    } catch (InvalidProtocolBufferException | IllegalArgumentException exception) {
      throw new IOException("Invalid protobuf DPCH packet", exception);
    }
  }

  static DpchPacket newDataPacket(long connectionId, int sequenceNumber, byte[] payload) {
    return DpchPacket.newBuilder().setConnectionId(connectionId).setPacketType(DpchPacketType.DPCH_PACKET_TYPE_DATA).setSequenceNumber(sequenceNumber)
        .setPayload(com.google.protobuf.ByteString.copyFrom(payload)).build();
  }

  static DpchPacket newAckPacket(long connectionId, int sequenceNumber) {
    return DpchPacket.newBuilder().setConnectionId(connectionId).setPacketType(DpchPacketType.DPCH_PACKET_TYPE_ACK).setSequenceNumber(sequenceNumber).build();
  }
}
