package pt.ulisboa.depchain.shared.network.links.perfect;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import pt.ulisboa.depchain.proto.DpchPacket;
import pt.ulisboa.depchain.proto.DpchPacketType;
import pt.ulisboa.depchain.shared.network.links.fairloss.FairLossLink;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;

@DisplayName("PerfectLink")
class PerfectLinkTest {
  private static final long CONNECTION_ID = 77L;

  @Test
  void deliversPacketsInOrderWhenDatagramsArriveOutOfOrder() throws Exception {
    InetSocketAddress receiverEndpoint = new InetSocketAddress(InetAddress.getLoopbackAddress(), freeUdpPort());
    byte[] firstPayload = "first-payload".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    byte[] secondPayload = "second-payload".getBytes(java.nio.charset.StandardCharsets.UTF_8);

    try (PerfectLink receiver = PerfectLink.bind(receiverEndpoint); FairLossLink rawSender = FairLossLink.unbound()) {
      rawSender.send(dataPacket(CONNECTION_ID, 1, secondPayload).toByteArray(), receiverEndpoint);

      assertThat(receiver.receive(150L)).isNull();

      rawSender.send(dataPacket(CONNECTION_ID, 0, firstPayload).toByteArray(), receiverEndpoint);

      InboundPacket firstDelivered = receiver.receive(1_000L);
      InboundPacket secondDelivered = receiver.receive(1_000L);

      assertThat(firstDelivered).isNotNull();
      assertThat(firstDelivered.packet().getSequenceNumber()).isEqualTo(0);
      assertThat(firstDelivered.payload().toByteArray()).isEqualTo(firstPayload);

      assertThat(secondDelivered).isNotNull();
      assertThat(secondDelivered.packet().getSequenceNumber()).isEqualTo(1);
      assertThat(secondDelivered.payload().toByteArray()).isEqualTo(secondPayload);
    }
  }

  @Test
  void dropsDuplicateDatagramsAfterFirstDelivery() throws Exception {
    InetSocketAddress receiverEndpoint = new InetSocketAddress(InetAddress.getLoopbackAddress(), freeUdpPort());
    byte[] payload = "deduplicated".getBytes(java.nio.charset.StandardCharsets.UTF_8);

    try (PerfectLink receiver = PerfectLink.bind(receiverEndpoint); FairLossLink rawSender = FairLossLink.unbound()) {
      byte[] datagram = dataPacket(CONNECTION_ID, 0, payload).toByteArray();
      rawSender.send(datagram, receiverEndpoint);
      rawSender.send(datagram, receiverEndpoint);

      InboundPacket delivered = receiver.receive(1_000L);

      assertThat(delivered).isNotNull();
      assertThat(delivered.packet().getSequenceNumber()).isZero();
      assertThat(delivered.payload().toByteArray()).isEqualTo(payload);
      assertThat(receiver.receive(200L)).isNull();
    }
  }

  @Test
  void buffersDuplicateOutOfOrderDatagramsOnlyOnce() throws Exception {
    InetSocketAddress receiverEndpoint = new InetSocketAddress(InetAddress.getLoopbackAddress(), freeUdpPort());
    byte[] firstPayload = "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    byte[] secondPayload = "beta".getBytes(java.nio.charset.StandardCharsets.UTF_8);

    try (PerfectLink receiver = PerfectLink.bind(receiverEndpoint); FairLossLink rawSender = FairLossLink.unbound()) {
      byte[] outOfOrder = dataPacket(CONNECTION_ID, 1, secondPayload).toByteArray();
      rawSender.send(outOfOrder, receiverEndpoint);
      rawSender.send(outOfOrder, receiverEndpoint);
      rawSender.send(dataPacket(CONNECTION_ID, 0, firstPayload).toByteArray(), receiverEndpoint);

      InboundPacket firstDelivered = receiver.receive(1_000L);
      InboundPacket secondDelivered = receiver.receive(1_000L);

      assertThat(firstDelivered.packet().getSequenceNumber()).isZero();
      assertThat(secondDelivered.packet().getSequenceNumber()).isEqualTo(1);
      assertThat(receiver.receive(200L)).isNull();
    }
  }

  private static DpchPacket dataPacket(long connectionId, int sequenceNumber, byte[] payload) {
    return DpchPacket.newBuilder().setConnectionId(connectionId).setSequenceNumber(sequenceNumber).setPacketType(DpchPacketType.DPCH_PACKET_TYPE_DATA)
        .setPayload(com.google.protobuf.ByteString.copyFrom(payload)).build();
  }

  private static int freeUdpPort() throws Exception {
    try (DatagramSocket socket = new DatagramSocket(0, InetAddress.getLoopbackAddress())) {
      return socket.getLocalPort();
    }
  }
}
