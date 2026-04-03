package pt.ulisboa.depchain.shared.network.links.stubborn;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.lang.reflect.Field;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.Test;

import pt.ulisboa.depchain.shared.network.links.fairloss.InboundBytes;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedKey;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedMessage;

class StubbornLinkTest {
  @Test
  void trackedMessageKeepsRetryingWhileLinkRemainsOpen() throws Exception {
    InetSocketAddress unusedEndpoint = new InetSocketAddress(InetAddress.getLoopbackAddress(), freeUdpPort());
    TrackedKey trackedKey = new TrackedKey(1L, 1, 1);

    try (StubbornLink link = StubbornLink.unbound()) {
      link.sendTracked(trackedKey, "retry-me".getBytes(java.nio.charset.StandardCharsets.UTF_8), unusedEndpoint);

      await().forever().untilAsserted(() -> assertThat(retryAttempt(link, unusedEndpoint, trackedKey)).isGreaterThanOrEqualTo(2));
      assertThat(link.pollTerminalFailure(trackedKey, unusedEndpoint)).isNull();
    }
  }

  @Test
  void cancelTrackedRemovesMessageFromRetryTracking() throws Exception {
    InetSocketAddress unusedEndpoint = new InetSocketAddress(InetAddress.getLoopbackAddress(), freeUdpPort());
    TrackedKey trackedKey = new TrackedKey(2L, 1, 1);

    try (StubbornLink link = StubbornLink.unbound()) {
      link.sendTracked(trackedKey, "cancel-me".getBytes(java.nio.charset.StandardCharsets.UTF_8), unusedEndpoint);
      link.cancelTracked(trackedKey, unusedEndpoint);

      await().during(Duration.ofMillis(500)).forever().untilAsserted(() -> assertThat(findTrackedMessage(link, unusedEndpoint, trackedKey)).isNull());
      assertThat(link.pollTerminalFailure(trackedKey, unusedEndpoint)).isNull();
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
        await().forever().untilAsserted(() -> {
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
        await().during(Duration.ofMillis(600)).forever().untilAsserted(() -> assertThat(receiver.receive(100L)).isNull());
      }
    }
  }

  private static int freeUdpPort() throws Exception {
    try (DatagramSocket socket = new DatagramSocket(0, InetAddress.getLoopbackAddress())) {
      return socket.getLocalPort();
    }
  }

  private static int retryAttempt(StubbornLink link, InetSocketAddress remoteEndpoint, TrackedKey trackedKey) throws Exception {
    TrackedMessage trackedMessage = findTrackedMessage(link, remoteEndpoint, trackedKey);
    assertThat(trackedMessage).isNotNull();
    return trackedMessage.retryAttempt();
  }

  @SuppressWarnings("unchecked")
  private static TrackedMessage findTrackedMessage(StubbornLink link, InetSocketAddress remoteEndpoint, TrackedKey trackedKey) throws Exception {
    Object context = readField(link, "context");
    Object retryState;
    synchronized (readField(context, "retryLock")) {
      Map<InetSocketAddress, Object> retryStatesByEndpoint = (Map<InetSocketAddress, Object>) readField(context, "retryStatesByEndpoint");
      retryState = retryStatesByEndpoint.get(remoteEndpoint);
      if (retryState == null) {
        return null;
      }

      Map<TrackedKey, TrackedMessage> trackedMessagesByKey = (Map<TrackedKey, TrackedMessage>) readField(retryState, "trackedMessagesByKey");
      return trackedMessagesByKey.get(trackedKey);
    }
  }

  private static Object readField(Object target, String name) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
  }
}
