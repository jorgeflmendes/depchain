package pt.ulisboa.depchain.shared.network.links.fairloss;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("benchmark")
class FairLossLinkBenchmarkTest {
  private static final int WARMUP_MESSAGES = 200;
  private static final int MEASURED_MESSAGES = 2_000;

  @Test
  void benchmarkSequentialSendReceive() throws Exception {
    InetSocketAddress receiverEndpoint = new InetSocketAddress(InetAddress.getLoopbackAddress(), freeUdpPort());
    byte[] payload = "fairloss-benchmark-payload".getBytes(StandardCharsets.UTF_8);

    try (FairLossLink receiver = FairLossLink.bind(receiverEndpoint); FairLossLink sender = FairLossLink.unbound()) {
      runRound(sender, receiver, receiverEndpoint, payload, WARMUP_MESSAGES);

      long startedAt = System.nanoTime();
      runRound(sender, receiver, receiverEndpoint, payload, MEASURED_MESSAGES);
      long elapsedNanos = System.nanoTime() - startedAt;

      System.out.println("FAIRLOSS_BENCHMARK elapsed_ms=" + (elapsedNanos / 1_000_000.0) + " messages=" + MEASURED_MESSAGES);
    }
  }

  private static void runRound(FairLossLink sender, FairLossLink receiver, InetSocketAddress receiverEndpoint, byte[] payload, int messageCount) throws Exception {
    for (int i = 0; i < messageCount; i++) {
      sender.send(payload, receiverEndpoint);
      InboundBytes inbound = receiver.receive(5_000L);
      assertNotNull(inbound);
      assertPayloadEquals(payload, inbound);
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
