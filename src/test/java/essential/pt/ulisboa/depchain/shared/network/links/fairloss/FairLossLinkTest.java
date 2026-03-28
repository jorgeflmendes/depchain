package pt.ulisboa.depchain.shared.network.links.fairloss;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class FairLossLinkTest {
  @AfterEach
  void clearSimulationProperties() {
    System.clearProperty(FairLossLink.DROP_PROBABILITY_PROPERTY);
    System.clearProperty(FairLossLink.DUPLICATE_PROBABILITY_PROPERTY);
    System.clearProperty(FairLossLink.ASYNC_MAX_DELAY_MS_PROPERTY);
  }

  @Test
  void receiveReturnsNullWhenTimeoutExpires() throws Exception {
    InetSocketAddress receiverEndpoint = new InetSocketAddress(InetAddress.getLoopbackAddress(), freeUdpPort());

    try (FairLossLink receiver = FairLossLink.bind(receiverEndpoint)) {
      assertThat(receiver.receive(50L)).isNull();
    }
  }

  @Test
  void sendDeliversInboundBytesToBoundReceiver() throws Exception {
    InetSocketAddress receiverEndpoint = new InetSocketAddress(InetAddress.getLoopbackAddress(), freeUdpPort());
    byte[] payload = "hello-netty".getBytes(java.nio.charset.StandardCharsets.UTF_8);

    try (FairLossLink receiver = FairLossLink.bind(receiverEndpoint); FairLossLink sender = FairLossLink.unbound()) {
      sender.send(payload, receiverEndpoint);

      InboundBytes inbound = receiver.receive(1_000L);

      assertThat(inbound).isNotNull();
      assertThat(inbound.sender()).isNotNull();
      assertThat(inbound.payload()).isEqualTo(payload);
    }
  }

  @Test
  void duplicateProbabilityCanEmitMoreThanOneDatagram() throws Exception {
    InetSocketAddress receiverEndpoint = new InetSocketAddress(InetAddress.getLoopbackAddress(), freeUdpPort());
    byte[] payload = "duplicated".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    System.setProperty(FairLossLink.DUPLICATE_PROBABILITY_PROPERTY, "1.0");

    try (FairLossLink receiver = FairLossLink.bind(receiverEndpoint); FairLossLink sender = FairLossLink.unbound()) {
      sender.send(payload, receiverEndpoint);

      InboundBytes firstInbound = receiver.receive(1_000L);
      InboundBytes secondInbound = receiver.receive(1_000L);

      assertThat(firstInbound).isNotNull();
      assertThat(secondInbound).isNotNull();
      assertThat(firstInbound.payload()).isEqualTo(payload);
      assertThat(secondInbound.payload()).isEqualTo(payload);
      assertThat(firstInbound.sender().getPort()).isPositive();
    }
  }

  @Test
  void dropProbabilityCanSuppressOutboundDatagrams() throws Exception {
    InetSocketAddress receiverEndpoint = new InetSocketAddress(InetAddress.getLoopbackAddress(), freeUdpPort());
    byte[] payload = "dropped".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    System.setProperty(FairLossLink.DROP_PROBABILITY_PROPERTY, "1.0");

    try (FairLossLink receiver = FairLossLink.bind(receiverEndpoint); FairLossLink sender = FairLossLink.unbound()) {
      sender.send(payload, receiverEndpoint);

      assertThat(receiver.receive(200L)).isNull();
    }
  }

  private static int freeUdpPort() throws Exception {
    try (DatagramSocket socket = new DatagramSocket(0, InetAddress.getLoopbackAddress())) {
      return socket.getLocalPort();
    }
  }
}
