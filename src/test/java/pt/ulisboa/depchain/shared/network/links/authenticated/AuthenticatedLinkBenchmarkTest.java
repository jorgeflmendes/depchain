package pt.ulisboa.depchain.shared.network.links.authenticated;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.PublicKey;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pt.ulisboa.depchain.shared.crypto.CryptoUtil;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;

@Tag("benchmark")
class AuthenticatedLinkBenchmarkTest {
  private static final int WARMUP_MESSAGES = 50;
  private static final int MEASURED_MESSAGES = 250;
  private static final long CONNECTION_ID = 1L;
  private static final long SENDER_ID = 1L;
  private static final long RECEIVER_ID = 2L;

  @Test
  void benchmarkRoundTripOnEstablishedAuthenticatedConnection() throws Exception {
    InetSocketAddress receiverEndpoint = new InetSocketAddress(InetAddress.getLoopbackAddress(), freeUdpPort());
    byte[] requestPayload = "authenticated-benchmark-request".getBytes(StandardCharsets.UTF_8);
    byte[] responsePayload = "authenticated-benchmark-response".getBytes(StandardCharsets.UTF_8);

    KeyPair senderStaticKeys = CryptoUtil.createEcKeyPair();
    KeyPair receiverStaticKeys = CryptoUtil.createEcKeyPair();
    Map<Long, PublicKey> staticPublicKeys = Map.of(SENDER_ID, senderStaticKeys.getPublic(), RECEIVER_ID, receiverStaticKeys.getPublic());

    try (AuthenticatedLink receiver = AuthenticatedLink.bind(receiverEndpoint, RECEIVER_ID, receiverStaticKeys.getPrivate(), staticPublicKeys);
        AuthenticatedLink sender = AuthenticatedLink.unbound(SENDER_ID, senderStaticKeys.getPrivate(), staticPublicKeys)) {
      runRound(sender, receiver, receiverEndpoint, requestPayload, responsePayload, WARMUP_MESSAGES);

      long startedAt = System.nanoTime();
      runRound(sender, receiver, receiverEndpoint, requestPayload, responsePayload, MEASURED_MESSAGES);
      long elapsedNanos = System.nanoTime() - startedAt;

      System.out.println("AUTHENTICATED_BENCHMARK elapsed_ms=" + (elapsedNanos / 1_000_000.0) + " messages=" + MEASURED_MESSAGES);
    }
  }

  private static void runRound(AuthenticatedLink sender, AuthenticatedLink receiver, InetSocketAddress receiverEndpoint, byte[] requestPayload, byte[] responsePayload, int messageCount)
      throws Exception {
    for (int i = 0; i < messageCount; i++) {
      sender.send(CONNECTION_ID, requestPayload, receiverEndpoint);

      InboundPacket request = receiver.receive(5_000L);
      assertNotNull(request);
      assertEquals(CONNECTION_ID, request.packet().getConnectionId());
      assertEquals(SENDER_ID, request.authenticatedSenderId());
      assertArrayEquals(requestPayload, request.payload().toByteArray());

      receiver.send(CONNECTION_ID, responsePayload, request.sender());

      InboundPacket response = sender.receive(5_000L);
      assertNotNull(response);
      assertEquals(CONNECTION_ID, response.packet().getConnectionId());
      assertEquals(RECEIVER_ID, response.authenticatedSenderId());
      assertArrayEquals(responsePayload, response.payload().toByteArray());
    }
  }

  private static int freeUdpPort() throws Exception {
    try (DatagramSocket socket = new DatagramSocket(0, InetAddress.getLoopbackAddress())) {
      return socket.getLocalPort();
    }
  }
}
