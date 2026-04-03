package pt.ulisboa.depchain.shared.network.links.authenticated;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.PublicKey;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import javax.crypto.spec.SecretKeySpec;

import org.junit.jupiter.api.Test;

import pt.ulisboa.depchain.shared.crypto.CryptoUtil;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;

class AuthenticatedLinkTest {
  private static final long CONNECTION_ID = 1L;
  private static final long SENDER_ID = 1L;
  private static final long RECEIVER_ID = 2L;

  @Test
  void receiveTimeoutTransitionsFromAsyncHandshakeToDirectReceive() throws Exception {
    InetSocketAddress receiverEndpoint = new InetSocketAddress(InetAddress.getLoopbackAddress(), freeUdpPort());
    byte[] requestPayload = "authenticated-test-request".getBytes(StandardCharsets.UTF_8);
    byte[] responsePayload = "authenticated-test-response".getBytes(StandardCharsets.UTF_8);

    KeyPair senderStaticKeys = CryptoUtil.createEcKeyPair();
    KeyPair receiverStaticKeys = CryptoUtil.createEcKeyPair();
    Map<Long, PublicKey> staticPublicKeys = Map.of(SENDER_ID, senderStaticKeys.getPublic(), RECEIVER_ID, receiverStaticKeys.getPublic());

    try (AuthenticatedLink receiver = AuthenticatedLink.bind(receiverEndpoint, RECEIVER_ID, receiverStaticKeys.getPrivate(), staticPublicKeys);
        AuthenticatedLink sender = AuthenticatedLink.unbound(SENDER_ID, senderStaticKeys.getPrivate(), staticPublicKeys)) {
      sender.send(CONNECTION_ID, requestPayload, receiverEndpoint);

      CompletableFuture<InboundPacket> responseFuture = new CompletableFuture<>();
      Thread.ofVirtual().start(() -> {
        try {
          responseFuture.complete(sender.receive(5_000L));
        } catch (Throwable throwable) {
          responseFuture.completeExceptionally(throwable);
        }
      });

      InboundPacket request = receiver.receive(5_000L);
      assertNotNull(request);
      assertEquals(CONNECTION_ID, request.packet().getConnectionId());
      assertEquals(SENDER_ID, request.authenticatedSenderId());
      assertArrayEquals(requestPayload, request.payload().toByteArray());

      receiver.send(CONNECTION_ID, responsePayload, request.sender());

      InboundPacket response = responseFuture.get(5L, TimeUnit.SECONDS);
      assertNotNull(response);
      assertEquals(CONNECTION_ID, response.packet().getConnectionId());
      assertEquals(RECEIVER_ID, response.authenticatedSenderId());
      assertArrayEquals(responsePayload, response.payload().toByteArray());
    }
  }

  @Test
  void connectionStateRejectsSkippedAndRepeatedNonces() {
    AuthenticatedConnectionState connectionState = new AuthenticatedConnectionState();
    connectionState.finishHandshake(new SecretKeySpec(new byte[32], "HmacSHA256"), RECEIVER_ID);

    assertTrue(connectionState.validateAndIncrementReceivedNonce(1L));
    assertFalse(connectionState.validateAndIncrementReceivedNonce(3L));
    assertFalse(connectionState.validateAndIncrementReceivedNonce(1L));
    assertTrue(connectionState.validateAndIncrementReceivedNonce(2L));
  }

  private static int freeUdpPort() throws Exception {
    try (DatagramSocket socket = new DatagramSocket(0, InetAddress.getLoopbackAddress())) {
      return socket.getLocalPort();
    }
  }
}
