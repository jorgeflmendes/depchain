package pt.ulisboa.depchain.shared.network.links.stubborn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pt.ulisboa.depchain.shared.network.links.fairloss.InboundBytes;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedKey;

@Tag("benchmark")
class StubbornLinkBenchmarkTest {
  private static final int WARMUP_MESSAGES = 100;
  private static final int MEASURED_MESSAGES = 500;

  @Test
  void benchmarkTrackedSendReceiveAndCancel() throws Exception {
    InetSocketAddress receiverEndpoint = new InetSocketAddress(InetAddress.getLoopbackAddress(), freeUdpPort());
    byte[] payload = "stubborn-benchmark-payload".getBytes(StandardCharsets.UTF_8);

    try (StubbornLink receiver = StubbornLink.bind(receiverEndpoint); StubbornLink sender = StubbornLink.unbound()) {
      runRound(sender, receiver, receiverEndpoint, payload, 0, WARMUP_MESSAGES);

      long startedAt = System.nanoTime();
      runRound(sender, receiver, receiverEndpoint, payload, WARMUP_MESSAGES, MEASURED_MESSAGES);
      long elapsedNanos = System.nanoTime() - startedAt;

      System.out.println("STUBBORN_BENCHMARK elapsed_ms=" + (elapsedNanos / 1_000_000.0) + " messages=" + MEASURED_MESSAGES);
    }
  }

  private static void runRound(StubbornLink sender, StubbornLink receiver, InetSocketAddress receiverEndpoint, byte[] payload, int startIndex, int messageCount) throws Exception {
    for (int i = 0; i < messageCount; i++) {
      TrackedKey trackedKey = new TrackedKey(startIndex + i + 1L, 1, 1);
      sender.sendTracked(trackedKey, payload, receiverEndpoint);

      InboundBytes inbound = receiver.receive(5_000L);
      assertNotNull(inbound);
      assertPayloadEquals(payload, inbound);

      sender.cancelTracked(trackedKey, receiverEndpoint);
    }
  }

  private static void assertPayloadEquals(byte[] expectedPayload, InboundBytes inbound) {
    assertEquals(expectedPayload.length, inbound.payloadLength());
    byte[] actualPayload = inbound.payloadView();
    for (int i = 0; i < expectedPayload.length; i++) {
      if (expectedPayload[i] != actualPayload[i]) {
        throw new AssertionError("Payload mismatch at byte index " + i);
      }
    }
  }

  private static int freeUdpPort() throws Exception {
    try (DatagramSocket socket = new DatagramSocket(0, InetAddress.getLoopbackAddress())) {
      return socket.getLocalPort();
    }
  }
}
