package pt.ulisboa.depchain.shared.network.links.authenticated;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.KeyPair;
import java.util.List;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.junit.jupiter.api.Test;

import pt.ulisboa.depchain.proto.AuthOpcode;
import pt.ulisboa.depchain.shared.crypto.CryptoUtil;

class AuthenticatedConnectionStateTest {
  @Test
  void planSendQueuesPayloadUntilHandshakeCompletes() throws Exception {
    AuthenticatedConnectionState state = new AuthenticatedConnectionState();
    byte[] payload = new byte[]{1, 2, 3};
    KeyPair localEphemeral = CryptoUtil.createEcKeyPair();
    SecretKey secretKey = new SecretKeySpec(new byte[32], "HmacSHA256");

    assertEquals(AuthenticatedConnectionState.SendAction.START_HANDSHAKE, state.planSend(payload));
    assertTrue(state.tryMarkHandshakeInitiated(localEphemeral.getPrivate()));
    assertEquals(AuthenticatedConnectionState.ReceiveMode.HANDSHAKE, state.receiveMode());

    List<byte[]> queuedPayloads = state.finishHandshake(secretKey, 42L);

    assertEquals(1, queuedPayloads.size());
    assertArrayEquals(payload, queuedPayloads.getFirst());
    assertTrue(state.isEstablished());
    assertEquals(42L, state.authenticatedRemoteSenderId());
    assertSame(secretKey, state.sharedSecret());
  }

  @Test
  void decideHandshakeRestartsOnlyForHigherSenderInit() throws Exception {
    AuthenticatedConnectionState state = new AuthenticatedConnectionState();
    KeyPair localEphemeral = CryptoUtil.createEcKeyPair();
    assertTrue(state.tryMarkHandshakeInitiated(localEphemeral.getPrivate()));

    assertEquals(AuthenticatedConnectionState.HandshakeAction.IGNORE, state.decideHandshake(AuthOpcode.AUTH_OPCODE_INIT, 4L, 5L));
    assertNotNull(state.ephemeralPrivateKey());

    assertEquals(AuthenticatedConnectionState.HandshakeAction.RESTART, state.decideHandshake(AuthOpcode.AUTH_OPCODE_INIT, 6L, 5L));
    assertNull(state.ephemeralPrivateKey());
    assertEquals(AuthenticatedConnectionState.ReceiveMode.INIT, state.receiveMode());
  }

  @Test
  void decideHandshakeUsesReplyDuringInitiatedPhase() throws Exception {
    AuthenticatedConnectionState state = new AuthenticatedConnectionState();
    KeyPair localEphemeral = CryptoUtil.createEcKeyPair();
    assertTrue(state.tryMarkHandshakeInitiated(localEphemeral.getPrivate()));

    assertEquals(AuthenticatedConnectionState.HandshakeAction.USE_REPLY, state.decideHandshake(AuthOpcode.AUTH_OPCODE_REPLY, 6L, 5L));
  }

  @Test
  void reserveSecureSendAndReceiveNonceAreMonotonic() {
    AuthenticatedConnectionState state = new AuthenticatedConnectionState();
    SecretKey secretKey = new SecretKeySpec(new byte[32], "HmacSHA256");
    state.finishHandshake(secretKey, 8L);

    assertEquals(1L, state.reserveSecureSend().nonce());
    assertEquals(2L, state.reserveSecureSend().nonce());

    assertTrue(state.validateAndIncrementReceivedNonce(1L));
    assertFalse(state.validateAndIncrementReceivedNonce(1L));
    assertFalse(state.validateAndIncrementReceivedNonce(3L));
    assertTrue(state.validateAndIncrementReceivedNonce(2L));
  }

  @Test
  void closeClearsEstablishedState() {
    AuthenticatedConnectionState state = new AuthenticatedConnectionState();
    SecretKey secretKey = new SecretKeySpec(new byte[32], "HmacSHA256");
    state.finishHandshake(secretKey, 12L);

    state.close();

    assertFalse(state.isEstablished());
    assertNull(state.sharedSecret());
    assertNull(state.authenticatedRemoteSenderId());
    assertThrows(IllegalStateException.class, state::reserveSecureSend);
  }
}
