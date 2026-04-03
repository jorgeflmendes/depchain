package pt.ulisboa.depchain.shared.network.links.perfect;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pt.ulisboa.depchain.shared.network.model.InboundPacket;

@Tag("benchmark")
class PerfectLinkBenchmarkTest {
  private static final int WARMUP_MESSAGES = 100;
  private static final int MEASURED_MESSAGES = 500;
  private static final long CONNECTION_ID = 1L;

  @Test
  void benchmarkRoundTripOnSingleConnection() throws Exception {
    InetSocketAddress receiverEndpoint = new InetSocketAddress(InetAddress.getLoopbackAddress(), freeUdpPort());
    byte[] requestPayload = "perfect-benchmark-request".getBytes(StandardCharsets.UTF_8);
    byte[] responsePayload = "perfect-benchmark-response".getBytes(StandardCharsets.UTF_8);

    try (PerfectLink receiver = PerfectLink.bind(receiverEndpoint); PerfectLink sender = PerfectLink.unbound()) {
      runRound(sender, receiver, receiverEndpoint, requestPayload, responsePayload, WARMUP_MESSAGES);

      long startedAt = System.nanoTime();
      runRound(sender, receiver, receiverEndpoint, requestPayload, responsePayload, MEASURED_MESSAGES);
      long elapsedNanos = System.nanoTime() - startedAt;

      System.out.println("PERFECT_BENCHMARK elapsed_ms=" + (elapsedNanos / 1_000_000.0) + " messages=" + MEASURED_MESSAGES);
    }
  }

  private static void runRound(PerfectLink sender, PerfectLink receiver, InetSocketAddress receiverEndpoint, byte[] requestPayload, byte[] responsePayload, int messageCount)
      throws Exception {
    for (int i = 0; i < messageCount; i++) {
      sender.send(CONNECTION_ID, requestPayload, receiverEndpoint);

      InboundPacket request = receiver.receive(5_000L);
      assertNotNull(request);
      assertEquals(CONNECTION_ID, request.packet().getConnectionId());
      assertArrayEquals(requestPayload, request.payload().toByteArray());

      receiver.send(CONNECTION_ID, responsePayload, request.sender());

      InboundPacket response = sender.receive(5_000L);
      assertNotNull(response);
      assertEquals(CONNECTION_ID, response.packet().getConnectionId());
      assertArrayEquals(responsePayload, response.payload().toByteArray());
    }
  }

  private static int freeUdpPort() throws Exception {
    try (DatagramSocket socket = new DatagramSocket(0, InetAddress.getLoopbackAddress())) {
      return socket.getLocalPort();
    }
  }
}
