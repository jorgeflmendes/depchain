package pt.ulisboa.depchain.shared.network.links.fairloss;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
      assertNull(receiver.receive(50L));
    }
  }

  @Test
  void sendDeliversInboundBytesToBoundReceiver() throws Exception {
    InetSocketAddress receiverEndpoint = new InetSocketAddress(InetAddress.getLoopbackAddress(), freeUdpPort());
    byte[] payload = "hello-netty".getBytes(java.nio.charset.StandardCharsets.UTF_8);

    try (FairLossLink receiver = FairLossLink.bind(receiverEndpoint); FairLossLink sender = FairLossLink.unbound()) {
      sender.send(payload, receiverEndpoint);

      InboundBytes inbound = receiver.receive(1_000L);

      assertNotNull(inbound.sender());
      assertArrayEquals(payload, inbound.payload());
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

      assertNotNull(firstInbound);
      assertNotNull(secondInbound);
      assertArrayEquals(payload, firstInbound.payload());
      assertArrayEquals(payload, secondInbound.payload());
      assertTrue(firstInbound.sender().getPort() > 0);
    }
  }

  private static int freeUdpPort() throws Exception {
    try (DatagramSocket socket = new DatagramSocket(0, InetAddress.getLoopbackAddress())) {
      return socket.getLocalPort();
    }
  }
}
