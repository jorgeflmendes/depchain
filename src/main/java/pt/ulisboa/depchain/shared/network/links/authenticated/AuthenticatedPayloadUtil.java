package pt.ulisboa.depchain.shared.network.links.authenticated;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.security.PrivateKey;
import java.security.PublicKey;

import javax.crypto.SecretKey;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import pt.ulisboa.depchain.proto.AuthOpcode;
import pt.ulisboa.depchain.proto.AuthenticatedDataEnvelope;
import pt.ulisboa.depchain.proto.AuthenticatedHandshakeEnvelope;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;
import pt.ulisboa.depchain.shared.utils.ProtoValidationUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class AuthenticatedPayloadUtil {
  public static final int HMAC_BYTES = 32;

  private AuthenticatedPayloadUtil() {
  }

  public static byte[] encodeEcdsa(AuthOpcode opcode, long senderId, PublicKey publicKey, PrivateKey privateKey) throws Exception {
    ValidationUtils.requireAllNonNull(named("opcode", opcode), named("publicKey", publicKey), named("privateKey", privateKey));
    byte[] publicKeyBytes = ValidationUtils.requirePresent(publicKey.getEncoded(), "publicKey must provide an encoded form");
    ValidationUtils.requirePositiveInt(publicKeyBytes.length, "publicKeyBytes.length");

    byte[] signedData = buildHandshakeSignaturePayload(opcode, senderId, publicKeyBytes);
    byte[] signature = CryptoUtil.signEcdsa(signedData, privateKey);
    AuthenticatedHandshakeEnvelope handshake = AuthenticatedHandshakeEnvelope.newBuilder().setAuthOpcode(opcode).setSenderId(senderId)
        .setEphemeralPublicKeyBytes(ByteString.copyFrom(publicKeyBytes)).setSignature(ByteString.copyFrom(signature)).build();
    return ProtoValidationUtil.requireValid(handshake, "AuthenticatedHandshakeEnvelope").toByteArray();
  }

  public static AuthenticatedHandshakeEnvelope decodeEcdsa(byte[] bytes) {
    ValidationUtils.requireNonNull(bytes, "bytes");
    return decodeEcdsa(ByteString.copyFrom(bytes));
  }

  public static AuthenticatedHandshakeEnvelope decodeEcdsa(ByteString bytes) {
    ValidationUtils.requireNonNull(bytes, "bytes");
    try {
      AuthenticatedHandshakeEnvelope handshake = ProtoValidationUtil.requireValid(AuthenticatedHandshakeEnvelope.parseFrom(bytes), "AuthenticatedHandshakeEnvelope");
      requireKnownOpcode(handshake.getAuthOpcode());
      ValidationUtils.requirePositiveInt(handshake.getEphemeralPublicKeyBytes().size(), "ephemeralPublicKeyBytes.length");
      ValidationUtils.requirePositiveInt(handshake.getSignature().size(), "signature.length");
      return handshake;
    } catch (InvalidProtocolBufferException exception) {
      throw new IllegalArgumentException("Invalid protobuf authenticated handshake payload", exception);
    }
  }

  public static boolean verifyEcdsa(byte[] bytes, PublicKey publicKey) throws Exception {
    ValidationUtils.requireNonNull(publicKey, "publicKey");
    return verifyEcdsa(decodeEcdsa(bytes), publicKey);
  }

  public static boolean verifyEcdsa(AuthenticatedHandshakeEnvelope handshake, PublicKey publicKey) throws Exception {
    ValidationUtils.requireAllNonNull(named("handshake", handshake), named("publicKey", publicKey));
    byte[] publicKeyBytes = handshake.getEphemeralPublicKeyBytes().toByteArray();
    byte[] signedData = buildHandshakeSignaturePayload(requireKnownOpcode(handshake.getAuthOpcode()), handshake.getSenderId(), publicKeyBytes);
    return CryptoUtil.verifyEcdsa(signedData, handshake.getSignature().toByteArray(), publicKey);
  }

  public static byte[] encodeHmac(AuthOpcode opcode, byte[] payload, SecretKey key, long nonce) throws Exception {
    ValidationUtils.requireAllNonNull(named("opcode", opcode), named("payload", payload), named("key", key));

    byte[] signablePayload = buildDataMacPayload(opcode, payload);
    byte[] hmac = CryptoUtil.signHmacWithNonce(signablePayload, key, nonce);
    AuthenticatedDataEnvelope data = AuthenticatedDataEnvelope.newBuilder().setAuthOpcode(opcode).setApplicationPayload(ByteString.copyFrom(payload)).setNonce(nonce)
        .setHmac(ByteString.copyFrom(hmac)).build();
    return ProtoValidationUtil.requireValid(data, "AuthenticatedDataEnvelope").toByteArray();
  }

  public static AuthenticatedDataEnvelope decodeHmac(byte[] bytes) {
    ValidationUtils.requireNonNull(bytes, "bytes");
    return decodeHmac(ByteString.copyFrom(bytes));
  }

  public static AuthenticatedDataEnvelope decodeHmac(ByteString bytes) {
    ValidationUtils.requireNonNull(bytes, "bytes");
    try {
      AuthenticatedDataEnvelope data = ProtoValidationUtil.requireValid(AuthenticatedDataEnvelope.parseFrom(bytes), "AuthenticatedDataEnvelope");
      requireKnownOpcode(data.getAuthOpcode());
      ValidationUtils.requireExactInt(data.getHmac().size(), HMAC_BYTES, "hmac.length");
      return data;
    } catch (InvalidProtocolBufferException exception) {
      throw new IllegalArgumentException("Invalid protobuf authenticated data payload", exception);
    }
  }

  public static boolean verifyHmac(byte[] bytes, SecretKey key) throws Exception {
    ValidationUtils.requireNonNull(key, "key");
    return verifyHmac(decodeHmac(bytes), key);
  }

  public static boolean verifyHmac(AuthenticatedDataEnvelope data, SecretKey key) throws Exception {
    ValidationUtils.requireAllNonNull(named("data", data), named("key", key));
    return CryptoUtil.verifyHmacWithNonce(buildDataMacPayload(requireKnownOpcode(data.getAuthOpcode()), data.getApplicationPayload().toByteArray()), data.getHmac()
        .toByteArray(), data.getNonce(), key);
  }

  private static byte[] buildHandshakeSignaturePayload(AuthOpcode opcode, long senderId, byte[] publicKeyBytes) {
    ValidationUtils.requireNonNull(opcode, "opcode");
    ValidationUtils.requirePositiveInt(publicKeyBytes.length, "publicKeyBytes.length");
    return AuthenticatedHandshakeEnvelope.newBuilder().setAuthOpcode(opcode).setSenderId(senderId).setEphemeralPublicKeyBytes(ByteString.copyFrom(publicKeyBytes)).build()
        .toByteArray();
  }

  private static byte[] buildDataMacPayload(AuthOpcode opcode, byte[] payload) {
    ValidationUtils.requireAllNonNull(named("opcode", opcode), named("payload", payload));
    return AuthenticatedDataEnvelope.newBuilder().setAuthOpcode(opcode).setApplicationPayload(ByteString.copyFrom(payload)).build().toByteArray();
  }

  private static AuthOpcode requireKnownOpcode(AuthOpcode opcode) {
    ValidationUtils.requireNonNull(opcode, "opcode");
    return switch (opcode) {
      case AUTH_OPCODE_INIT, AUTH_OPCODE_REPLY, AUTH_OPCODE_DATA -> opcode;
      case AUTH_OPCODE_UNSPECIFIED -> throw new IllegalArgumentException("Authentication opcode is unspecified");
      case UNRECOGNIZED -> throw new IllegalArgumentException("Authentication opcode is unrecognized");
    };
  }
}
