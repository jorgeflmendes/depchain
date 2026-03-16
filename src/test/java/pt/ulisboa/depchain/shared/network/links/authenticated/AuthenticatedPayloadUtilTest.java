package pt.ulisboa.depchain.shared.network.links.authenticated;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.security.KeyPair;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;

import pt.ulisboa.depchain.proto.AuthOpcode;
import pt.ulisboa.depchain.proto.AuthenticatedDataEnvelope;
import pt.ulisboa.depchain.proto.AuthenticatedEnvelope;
import pt.ulisboa.depchain.proto.AuthenticatedHandshakeEnvelope;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;

class AuthenticatedPayloadUtilTest {
  @Test
  void handshakeRoundTripVerifiesSignature() throws Exception {
    KeyPair staticKeyPair = CryptoUtil.newECKeyPair();
    KeyPair ephemeralKeyPair = CryptoUtil.newECKeyPair();

    byte[] encoded = AuthenticatedPayloadUtil.encodeEcdsa(AuthOpcode.AUTH_OPCODE_INIT, 17L, ephemeralKeyPair.getPublic(), staticKeyPair.getPrivate());

    AuthenticatedHandshakeEnvelope decoded = AuthenticatedPayloadUtil.decodeEcdsa(encoded);

    assertEquals(AuthOpcode.AUTH_OPCODE_INIT, decoded.getAuthOpcode());
    assertEquals(17L, decoded.getSenderId());
    assertArrayEquals(ephemeralKeyPair.getPublic().getEncoded(), decoded.getEphemeralPublicKeyBytes().toByteArray());
    assertTrue(AuthenticatedPayloadUtil.verifyEcdsa(decoded, staticKeyPair.getPublic()));
  }

  @Test
  void tamperedHandshakeSignatureFailsVerification() throws Exception {
    KeyPair staticKeyPair = CryptoUtil.newECKeyPair();
    KeyPair ephemeralKeyPair = CryptoUtil.newECKeyPair();
    byte[] encoded = AuthenticatedPayloadUtil.encodeEcdsa(AuthOpcode.AUTH_OPCODE_REPLY, 19L, ephemeralKeyPair.getPublic(), staticKeyPair.getPrivate());

    AuthenticatedHandshakeEnvelope tamperedHandshake = AuthenticatedEnvelope.parseFrom(encoded).getHandshake().toBuilder().setSenderId(20L).build();
    assertFalse(AuthenticatedPayloadUtil.verifyEcdsa(tamperedHandshake, staticKeyPair.getPublic()));
  }

  @Test
  void decodeHandshakeRejectsDataEnvelope() throws Exception {
    SecretKey secretKey = new SecretKeySpec(new byte[32], "HmacSHA256");
    byte[] encoded = AuthenticatedPayloadUtil.encodeHmac(AuthOpcode.AUTH_OPCODE_DATA, "payload".getBytes(StandardCharsets.UTF_8), secretKey, 1L);

    assertThrows(IllegalArgumentException.class, () -> AuthenticatedPayloadUtil.decodeEcdsa(encoded));
  }

  @Test
  void dataRoundTripVerifiesHmac() throws Exception {
    KeyPair senderKeyPair = CryptoUtil.newECKeyPair();
    KeyPair receiverKeyPair = CryptoUtil.newECKeyPair();
    SecretKey secretKey = CryptoUtil.deriveCommonKey(senderKeyPair.getPrivate(), receiverKeyPair.getPublic(), new CryptoUtil.KeyContext("authenticated", "test"));
    byte[] payload = "application-payload".getBytes(StandardCharsets.UTF_8);

    byte[] encoded = AuthenticatedPayloadUtil.encodeHmac(AuthOpcode.AUTH_OPCODE_DATA, payload, secretKey, 3L);
    AuthenticatedDataEnvelope decoded = AuthenticatedPayloadUtil.decodeHmac(encoded);

    assertEquals(3L, decoded.getNonce());
    assertArrayEquals(payload, decoded.getApplicationPayload().toByteArray());
    assertTrue(AuthenticatedPayloadUtil.verifyHmac(decoded, secretKey));
  }

  @Test
  void tamperedDataNonceOrPayloadFailsVerification() throws Exception {
    KeyPair senderKeyPair = CryptoUtil.newECKeyPair();
    KeyPair receiverKeyPair = CryptoUtil.newECKeyPair();
    SecretKey secretKey = CryptoUtil.deriveCommonKey(senderKeyPair.getPrivate(), receiverKeyPair.getPublic(), new CryptoUtil.KeyContext("authenticated", "tamper-test"));
    byte[] encoded = AuthenticatedPayloadUtil.encodeHmac(AuthOpcode.AUTH_OPCODE_DATA, "payload".getBytes(StandardCharsets.UTF_8), secretKey, 5L);
    AuthenticatedDataEnvelope decoded = AuthenticatedPayloadUtil.decodeHmac(encoded);

    AuthenticatedDataEnvelope tamperedNonce = decoded.toBuilder().setNonce(6L).build();
    AuthenticatedDataEnvelope tamperedPayload = decoded.toBuilder().setApplicationPayload(ByteString.copyFromUtf8("other")).build();

    assertFalse(AuthenticatedPayloadUtil.verifyHmac(tamperedNonce, secretKey));
    assertFalse(AuthenticatedPayloadUtil.verifyHmac(tamperedPayload, secretKey));
  }

  @Test
  void decodeDataRejectsWrongHmacLength() {
    AuthenticatedEnvelope invalidEnvelope = AuthenticatedEnvelope.newBuilder().setData(AuthenticatedDataEnvelope.newBuilder().setAuthOpcode(AuthOpcode.AUTH_OPCODE_DATA)
        .setApplicationPayload(ByteString.copyFromUtf8("payload")).setNonce(1L).setHmac(ByteString.copyFrom(new byte[31]))).build();

    assertThrows(IllegalArgumentException.class, () -> AuthenticatedPayloadUtil.decodeHmac(invalidEnvelope.toByteArray()));
  }
}
