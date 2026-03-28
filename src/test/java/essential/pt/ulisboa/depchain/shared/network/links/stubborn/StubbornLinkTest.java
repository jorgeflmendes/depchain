package pt.ulisboa.depchain.shared.network.links.stubborn;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;

import org.junit.jupiter.api.Test;

import pt.ulisboa.depchain.shared.network.links.fairloss.InboundBytes;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedKey;

class StubbornLinkTest {
  @Test
  void trackedMessageDoesNotReportTerminalFailureWhileLinkRemainsOpen() throws Exception {
    InetSocketAddress unusedEndpoint = new InetSocketAddress(InetAddress.getLoopbackAddress(), freeUdpPort());
    TrackedKey trackedKey = new TrackedKey(1L, 1, 1);

    try (StubbornLink link = StubbornLink.unbound()) {
      link.sendTracked(trackedKey, "retry-me".getBytes(java.nio.charset.StandardCharsets.UTF_8), unusedEndpoint);

      await().during(Duration.ofMillis(800)).atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertThat(link.pollTerminalFailure(trackedKey, unusedEndpoint)).isNull());
    }
  }

  @Test
  void cancelTrackedStopsFutureTerminalFailure() throws Exception {
    InetSocketAddress unusedEndpoint = new InetSocketAddress(InetAddress.getLoopbackAddress(), freeUdpPort());
    TrackedKey trackedKey = new TrackedKey(2L, 1, 1);

    try (StubbornLink link = StubbornLink.unbound()) {
      link.sendTracked(trackedKey, "cancel-me".getBytes(java.nio.charset.StandardCharsets.UTF_8), unusedEndpoint);
      link.cancelTracked(trackedKey, unusedEndpoint);

      await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(1)).untilAsserted(() -> assertThat(link.pollTerminalFailure(trackedKey, unusedEndpoint)).isNull());
    }
  }

  @Test
  void trackedMessageIsEventuallyDeliveredAfterReceiverStartsListening() throws Exception {
    InetSocketAddress delayedReceiverEndpoint = new InetSocketAddress(InetAddress.getLoopbackAddress(), freeUdpPort());
    TrackedKey trackedKey = new TrackedKey(3L, 1, 1);
    byte[] payload = "eventual-delivery".getBytes(java.nio.charset.StandardCharsets.UTF_8);

    try (StubbornLink sender = StubbornLink.unbound()) {
      sender.sendTracked(trackedKey, payload, delayedReceiverEndpoint);

      try (StubbornLink receiver = StubbornLink.bind(delayedReceiverEndpoint)) {
        await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
          InboundBytes inbound = receiver.receive(250L);
          assertThat(inbound).isNotNull();
          assertThat(inbound.payload()).isEqualTo(payload);
        });
      }
    }
  }

  @Test
  void cancelledTrackedMessageIsNotDeliveredAfterReceiverStartsListening() throws Exception {
    InetSocketAddress delayedReceiverEndpoint = new InetSocketAddress(InetAddress.getLoopbackAddress(), freeUdpPort());
    TrackedKey trackedKey = new TrackedKey(4L, 1, 1);
    byte[] payload = "cancelled-before-delivery".getBytes(java.nio.charset.StandardCharsets.UTF_8);

    try (StubbornLink sender = StubbornLink.unbound()) {
      sender.sendTracked(trackedKey, payload, delayedReceiverEndpoint);
      sender.cancelTracked(trackedKey, delayedReceiverEndpoint);

      try (StubbornLink receiver = StubbornLink.bind(delayedReceiverEndpoint)) {
        await().during(Duration.ofMillis(600)).atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertThat(receiver.receive(100L)).isNull());
      }
    }
  }

  private static int freeUdpPort() throws Exception {
    try (DatagramSocket socket = new DatagramSocket(0, InetAddress.getLoopbackAddress())) {
      return socket.getLocalPort();
    }
  }
}
