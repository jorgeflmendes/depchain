package pt.ulisboa.depchain.shared.network.links.authenticated;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.nio.ByteBuffer;
import java.security.PrivateKey;
import java.security.PublicKey;

import javax.crypto.SecretKey;

import pt.ulisboa.depchain.shared.utils.CryptoUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class AuthenticatedPayload {
  public static final int OPCODE_BYTES = 1;
  public static final int LENGTH_BYTES = Integer.BYTES;
  public static final int HMAC_BYTES = 32;

  public record DecodedHmacPayload(AuthOpcode opcode, byte[] payload, long nonce, byte[] hmac) {
    public DecodedHmacPayload {
      ValidationUtils.requireAllNonNull(named("opcode", opcode), named("payload", payload), named("hmac", hmac));
      ValidationUtils.requireExactInt(hmac.length, HMAC_BYTES, "hmac.length");
    }
  }

  public record DecodedEcdsaPayload(AuthOpcode opcode, long senderId, byte[] publicKeyBytes, byte[] signature) {
    public DecodedEcdsaPayload {
      ValidationUtils.requireAllNonNull(named("opcode", opcode), named("publicKeyBytes", publicKeyBytes), named("signature", signature));
      ValidationUtils.requirePositiveInt(publicKeyBytes.length, "publicKeyBytes.length");
      ValidationUtils.requirePositiveInt(signature.length, "signature.length");
    }
  }

  public static byte[] encodeEcdsa(AuthOpcode opcode, long senderId, PublicKey publicKey, PrivateKey privateKey) throws Exception {
    ValidationUtils.requireAllNonNull(named("opcode", opcode), named("publicKey", publicKey), named("privateKey", privateKey));
    byte[] publicKeyBytes = ValidationUtils.requirePresent(publicKey.getEncoded(), "publicKey must provide an encoded form");
    ValidationUtils.requirePositiveInt(publicKeyBytes.length, "publicKeyBytes.length");

    byte[] signedData = buildEcdsaMessage(opcode, senderId, publicKeyBytes);
    byte[] signature = CryptoUtil.signEcdsa(signedData, privateKey);

    return appendOpcodeAndSignature(opcode, senderId, publicKeyBytes, signature);
  }

  public static DecodedEcdsaPayload decodeEcdsa(byte[] bytes) {
    ValidationUtils.requireNonNull(bytes, "bytes");
    int minLength = OPCODE_BYTES + Long.BYTES + LENGTH_BYTES + 1 + LENGTH_BYTES + 1;
    ValidationUtils.requireAtLeastInt(bytes.length, minLength, "bytes length", Integer.toString(minLength));
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    AuthOpcode opcode = AuthOpcode.fromCode(buffer.get());
    validateHandshakeOpcode(opcode);
    long senderId = buffer.getLong();

    byte[] publicKeyBytes = getLengthPrefixedBytes(buffer, "publicKeyBytes");
    byte[] signature = getLengthPrefixedBytes(buffer, "signature");
    if (buffer.hasRemaining()) {
      throw new IllegalArgumentException("Unexpected trailing bytes in authenticated handshake payload");
    }

    return new DecodedEcdsaPayload(opcode, senderId, publicKeyBytes, signature);
  }

  public static boolean verifyEcdsa(byte[] bytes, PublicKey publicKey) throws Exception {
    ValidationUtils.requireNonNull(publicKey, "publicKey");

    DecodedEcdsaPayload decoded = decodeEcdsa(bytes);
    return verifyEcdsa(decoded, publicKey);
  }

  public static boolean verifyEcdsa(DecodedEcdsaPayload decoded, PublicKey publicKey) throws Exception {
    ValidationUtils.requireAllNonNull(named("decoded", decoded), named("publicKey", publicKey));
    byte[] signedData = buildEcdsaMessage(decoded.opcode(), decoded.senderId(), decoded.publicKeyBytes());
    return CryptoUtil.verifyEcdsa(signedData, decoded.signature(), publicKey);
  }

  public static byte[] encodeHmac(AuthOpcode opcode, byte[] payload, SecretKey key, long nonce) throws Exception {
    ValidationUtils.requireAllNonNull(named("opcode", opcode), named("payload", payload), named("key", key));

    byte[] hmac = CryptoUtil.signHmacWithNonce(buildMessageWithOpcode(opcode, payload), key, nonce);

    return appendOpcodeNonceAndHmac(opcode, payload, nonce, hmac);
  }

  public static DecodedHmacPayload decodeHmac(byte[] bytes) {
    ValidationUtils.requireNonNull(bytes, "bytes");
    int minLength = OPCODE_BYTES + LENGTH_BYTES + 1 + Long.BYTES + LENGTH_BYTES + 1;
    ValidationUtils.requireAtLeastInt(bytes.length, minLength, "bytes length", Integer.toString(minLength));
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    AuthOpcode opcode = AuthOpcode.fromCode(buffer.get());
    validateSecureDataOpcode(opcode);

    byte[] payload = getLengthPrefixedBytes(buffer, "payload");
    long nonce = buffer.getLong();
    byte[] hmac = getLengthPrefixedBytes(buffer, "hmac");
    ValidationUtils.requireExactInt(hmac.length, HMAC_BYTES, "hmac.length");
    if (buffer.hasRemaining()) {
      throw new IllegalArgumentException("Unexpected trailing bytes in authenticated secure payload");
    }

    return new DecodedHmacPayload(opcode, payload, nonce, hmac);
  }

  public static boolean verifyHmac(byte[] bytes, SecretKey key) throws Exception {
    ValidationUtils.requireNonNull(key, "key");

    DecodedHmacPayload decoded = decodeHmac(bytes);
    return verifyHmac(decoded, key);
  }

  public static boolean verifyHmac(DecodedHmacPayload decoded, SecretKey key) throws Exception {
    ValidationUtils.requireAllNonNull(named("decoded", decoded), named("key", key));
    return CryptoUtil.verifyHmacWithNonce(buildMessageWithOpcode(decoded.opcode(), decoded.payload()), decoded.hmac(), decoded.nonce(), key);
  }

  private static byte[] appendOpcodeAndSignature(AuthOpcode opcode, long senderId, byte[] publicKeyBytes, byte[] signature) {
    ValidationUtils.requireAllNonNull(named("publicKeyBytes", publicKeyBytes), named("signature", signature));
    ValidationUtils.requirePositiveInt(publicKeyBytes.length, "publicKeyBytes.length");
    ValidationUtils.requirePositiveInt(signature.length, "signature.length");

    ByteBuffer buffer = ByteBuffer.allocate(OPCODE_BYTES + Long.BYTES + LENGTH_BYTES + publicKeyBytes.length + LENGTH_BYTES + signature.length);
    buffer.put(opcode.code());
    buffer.putLong(senderId);
    putLengthPrefixedBytes(buffer, publicKeyBytes);
    putLengthPrefixedBytes(buffer, signature);

    return buffer.array();
  }

  private static byte[] appendOpcodeNonceAndHmac(AuthOpcode opcode, byte[] payload, long nonce, byte[] hmac) {
    ValidationUtils.requireExactInt(hmac.length, HMAC_BYTES, "hmac length");

    ByteBuffer buffer = ByteBuffer.allocate(OPCODE_BYTES + LENGTH_BYTES + payload.length + Long.BYTES + LENGTH_BYTES + hmac.length);
    buffer.put(opcode.code());
    putLengthPrefixedBytes(buffer, payload);
    buffer.putLong(nonce);
    putLengthPrefixedBytes(buffer, hmac);

    return buffer.array();
  }

  private static byte[] buildMessageWithOpcode(AuthOpcode opcode, byte[] payload) {
    ByteBuffer buffer = ByteBuffer.allocate(OPCODE_BYTES + payload.length);
    buffer.put(opcode.code());
    buffer.put(payload);

    return buffer.array();
  }

  private static byte[] buildEcdsaMessage(AuthOpcode opcode, long senderId, byte[] publicKeyBytes) {
    ValidationUtils.requirePositiveInt(publicKeyBytes.length, "publicKeyBytes.length");
    ByteBuffer buffer = ByteBuffer.allocate(OPCODE_BYTES + Long.BYTES + LENGTH_BYTES + publicKeyBytes.length);
    buffer.put(opcode.code());
    buffer.putLong(senderId);
    putLengthPrefixedBytes(buffer, publicKeyBytes);

    return buffer.array();
  }

  private static void putLengthPrefixedBytes(ByteBuffer buffer, byte[] bytes) {
    ValidationUtils.requireAllNonNull(named("buffer", buffer), named("bytes", bytes));
    buffer.putInt(bytes.length);
    buffer.put(bytes);
  }

  private static byte[] getLengthPrefixedBytes(ByteBuffer buffer, String fieldName) {
    ValidationUtils.requireAllNonNull(named("buffer", buffer), named("fieldName", fieldName));

    if (buffer.remaining() < LENGTH_BYTES) {
      throw new IllegalArgumentException("Missing length for " + fieldName);
    }

    int length = buffer.getInt();
    ValidationUtils.requirePositiveInt(length, fieldName + ".length");
    ValidationUtils.requireAtMostInt(length, buffer.remaining(), fieldName + ".length");

    byte[] bytes = new byte[length];
    buffer.get(bytes);
    return bytes;
  }

  private static void validateSecureDataOpcode(AuthOpcode opcode) {
    if (opcode != AuthOpcode.DATA) {
      throw new IllegalArgumentException("HMAC payload must use DATA opcode");
    }
  }

  private static void validateHandshakeOpcode(AuthOpcode opcode) {
    if (opcode != AuthOpcode.INIT && opcode != AuthOpcode.REPLY) {
      throw new IllegalArgumentException("ECDSA payload must use INIT or REPLY opcode");
    }
  }
}

