package pt.ulisboa.depchain.shared.network.links.stubborn;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;

import org.junit.jupiter.api.Test;

import pt.ulisboa.depchain.shared.network.links.LinkFailureException;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedKey;

class StubbornLinkTest {
  @Test
  void trackedMessageEventuallyReportsTerminalFailure() throws Exception {
    InetSocketAddress unusedEndpoint = new InetSocketAddress(InetAddress.getLoopbackAddress(), freeUdpPort());
    TrackedKey trackedKey = new TrackedKey(1L, 1, 1);

    try (StubbornLink link = StubbornLink.unbound()) {
      link.sendTracked(trackedKey, "retry-me".getBytes(java.nio.charset.StandardCharsets.UTF_8), unusedEndpoint);

      await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
        LinkFailureException failure = link.pollTerminalFailure(trackedKey, unusedEndpoint);
        assertNotNull(failure);
      });
    }
  }

  @Test
  void cancelTrackedStopsFutureTerminalFailure() throws Exception {
    InetSocketAddress unusedEndpoint = new InetSocketAddress(InetAddress.getLoopbackAddress(), freeUdpPort());
    TrackedKey trackedKey = new TrackedKey(2L, 1, 1);

    try (StubbornLink link = StubbornLink.unbound()) {
      link.sendTracked(trackedKey, "cancel-me".getBytes(java.nio.charset.StandardCharsets.UTF_8), unusedEndpoint);
      link.cancelTracked(trackedKey, unusedEndpoint);

      await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(1)).untilAsserted(() -> {
        assertNull(link.pollTerminalFailure(trackedKey, unusedEndpoint));
      });
    }
  }

  private static int freeUdpPort() throws Exception {
    try (DatagramSocket socket = new DatagramSocket(0, InetAddress.getLoopbackAddress())) {
      return socket.getLocalPort();
    }
  }
}
