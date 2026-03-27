package pt.ulisboa.depchain.shared.network.links.authenticated;

import static pt.ulisboa.depchain.shared.validation.ValidationUtils.named;

import java.security.PrivateKey;
import java.security.PublicKey;

import javax.crypto.SecretKey;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;

import pt.ulisboa.depchain.proto.AuthOpcode;
import pt.ulisboa.depchain.proto.AuthenticatedDataEnvelope;
import pt.ulisboa.depchain.proto.AuthenticatedHandshakeEnvelope;
import pt.ulisboa.depchain.shared.crypto.CryptoUtil;
import pt.ulisboa.depchain.shared.validation.ValidationUtils;

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
    return buildHandshakeEnvelope(opcode, senderId, publicKeyBytes, signature);
  }

  public static AuthenticatedHandshakeEnvelope decodeEcdsa(byte[] bytes) {
    ValidationUtils.requireNonNull(bytes, "bytes");
    return decodeEcdsa(ByteString.copyFrom(bytes));
  }

  public static AuthenticatedHandshakeEnvelope decodeEcdsa(ByteString bytes) {
    ValidationUtils.requireNonNull(bytes, "bytes");
    try {
      AuthenticatedHandshakeEnvelope handshake = AuthenticatedHandshakeEnvelope.parseFrom(bytes);
      validateHandshakeEnvelope(handshake);
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

    AuthOpcode knownOpcode = requireKnownOpcode(opcode);
    byte[] signablePayload = buildDataMacPayload(knownOpcode, payload);
    byte[] hmac = CryptoUtil.signHmacWithNonce(signablePayload, key, nonce);
    return buildDataEnvelope(knownOpcode, payload, nonce, hmac);
  }

  public static AuthenticatedDataEnvelope decodeHmac(byte[] bytes) {
    ValidationUtils.requireNonNull(bytes, "bytes");
    return decodeHmac(ByteString.copyFrom(bytes));
  }

  public static AuthenticatedDataEnvelope decodeHmac(ByteString bytes) {
    ValidationUtils.requireNonNull(bytes, "bytes");
    try {
      AuthenticatedDataEnvelope data = AuthenticatedDataEnvelope.parseFrom(bytes);
      validateDataEnvelope(data);
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
    validateDataEnvelope(data);
    return CryptoUtil.verifyHmacWithNonce(buildDataMacPayload(data.getAuthOpcode(), data.getApplicationPayload()), data.getHmac().toByteArray(), data.getNonce(), key);
  }

  private static byte[] buildHandshakeSignaturePayload(AuthOpcode opcode, long senderId, byte[] publicKeyBytes) {
    AuthOpcode knownOpcode = requireKnownOpcode(opcode);
    ValidationUtils.requirePositiveInt(publicKeyBytes.length, "publicKeyBytes.length");
    return writeHandshakeEnvelopeBytes(knownOpcode, senderId, publicKeyBytes, null);
  }

  private static byte[] buildDataMacPayload(AuthOpcode opcode, byte[] payload) {
    ValidationUtils.requireNonNull(payload, "payload");
    return writeDataEnvelopeBytes(requireKnownOpcode(opcode), payload, null, null, null);
  }

  private static byte[] buildDataMacPayload(AuthOpcode opcode, ByteString payload) {
    ValidationUtils.requireNonNull(payload, "payload");
    return writeDataEnvelopeBytes(requireKnownOpcode(opcode), null, payload, null, null);
  }

  private static byte[] buildHandshakeEnvelope(AuthOpcode opcode, long senderId, byte[] publicKeyBytes, byte[] signature) {
    ValidationUtils.requirePositiveInt(publicKeyBytes.length, "publicKeyBytes.length");
    ValidationUtils.requirePositiveInt(signature.length, "signature.length");
    return writeHandshakeEnvelopeBytes(requireKnownOpcode(opcode), senderId, publicKeyBytes, signature);
  }

  private static byte[] buildDataEnvelope(AuthOpcode opcode, byte[] payload, long nonce, byte[] hmac) {
    ValidationUtils.requireAllNonNull(named("payload", payload), named("hmac", hmac));
    ValidationUtils.requireExactInt(hmac.length, HMAC_BYTES, "hmac.length");
    return writeDataEnvelopeBytes(opcode, payload, null, nonce, hmac);
  }

  private static void validateHandshakeEnvelope(AuthenticatedHandshakeEnvelope handshake) {
    ValidationUtils.requireNonNull(handshake, "handshake");
    if (!handshake.hasAuthOpcode()) {
      throw new IllegalArgumentException("authOpcode is required");
    }
    requireKnownOpcode(handshake.getAuthOpcode());
    if (!handshake.hasSenderId()) {
      throw new IllegalArgumentException("senderId is required");
    }
    if (!handshake.hasEphemeralPublicKeyBytes()) {
      throw new IllegalArgumentException("ephemeralPublicKeyBytes is required");
    }
    ValidationUtils.requirePositiveInt(handshake.getEphemeralPublicKeyBytes().size(), "ephemeralPublicKeyBytes.length");
    if (!handshake.hasSignature()) {
      throw new IllegalArgumentException("signature is required");
    }
    ValidationUtils.requirePositiveInt(handshake.getSignature().size(), "signature.length");
  }

  private static void validateDataEnvelope(AuthenticatedDataEnvelope data) {
    ValidationUtils.requireNonNull(data, "data");
    if (!data.hasAuthOpcode()) {
      throw new IllegalArgumentException("authOpcode is required");
    }
    AuthOpcode opcode = requireKnownOpcode(data.getAuthOpcode());
    if (opcode != AuthOpcode.AUTH_OPCODE_DATA) {
      throw new IllegalArgumentException("authenticated data envelope must use DATA opcode");
    }
    if (!data.hasApplicationPayload()) {
      throw new IllegalArgumentException("applicationPayload is required");
    }
    if (!data.hasNonce()) {
      throw new IllegalArgumentException("nonce is required");
    }
    if (!data.hasHmac()) {
      throw new IllegalArgumentException("hmac is required");
    }
    ValidationUtils.requireExactInt(data.getHmac().size(), HMAC_BYTES, "hmac.length");
  }

  private static byte[] writeHandshakeEnvelopeBytes(AuthOpcode opcode, long senderId, byte[] publicKeyBytes, byte[] signature) {
    int size = CodedOutputStream.computeEnumSize(1, opcode.getNumber()) + CodedOutputStream.computeUInt64Size(2, senderId)
        + CodedOutputStream.computeByteArraySize(3, publicKeyBytes);
    if (signature != null) {
      size += CodedOutputStream.computeByteArraySize(4, signature);
    }

    byte[] bytes = new byte[size];
    try {
      CodedOutputStream output = CodedOutputStream.newInstance(bytes);
      output.writeEnum(1, opcode.getNumber());
      output.writeUInt64(2, senderId);
      output.writeByteArray(3, publicKeyBytes);
      if (signature != null) {
        output.writeByteArray(4, signature);
      }
      output.flush();
      return bytes;
    } catch (Exception exception) {
      throw new IllegalStateException("Failed to encode authenticated handshake payload", exception);
    }
  }

  private static byte[] writeDataEnvelopeBytes(AuthOpcode opcode, byte[] payloadBytes, ByteString payloadByteString, Long nonce, byte[] hmac) {
    int size = CodedOutputStream.computeEnumSize(1, opcode.getNumber());
    size += payloadBytes != null ? CodedOutputStream.computeByteArraySize(2, payloadBytes) : CodedOutputStream.computeBytesSize(2, payloadByteString);
    if (nonce != null) {
      size += CodedOutputStream.computeUInt64Size(3, nonce.longValue());
    }
    if (hmac != null) {
      size += CodedOutputStream.computeByteArraySize(4, hmac);
    }

    byte[] bytes = new byte[size];
    try {
      CodedOutputStream output = CodedOutputStream.newInstance(bytes);
      output.writeEnum(1, opcode.getNumber());
      if (payloadBytes != null) {
        output.writeByteArray(2, payloadBytes);
      } else {
        output.writeBytes(2, payloadByteString);
      }
      if (nonce != null) {
        output.writeUInt64(3, nonce.longValue());
      }
      if (hmac != null) {
        output.writeByteArray(4, hmac);
      }
      output.flush();
      return bytes;
    } catch (Exception exception) {
      throw new IllegalStateException("Failed to encode authenticated data payload", exception);
    }
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
