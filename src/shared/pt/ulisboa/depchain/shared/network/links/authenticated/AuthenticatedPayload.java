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
  public static final int HMAC_BYTES = 32;
  public static final int EPHEMERAL_PUBLIC_KEY_BYTES = 91;

  public record DecodedHmacPayload(AuthOpcode opcode, byte[] payload, long nonce, byte[] hmac) {
    public DecodedHmacPayload {
      ValidationUtils.requireAllNonNull(named("opcode", opcode), named("payload", payload), named("hmac", hmac));
      ValidationUtils.requireExactInt(hmac.length, HMAC_BYTES, "hmac.length");
    }
  }

  public record DecodedEcdsaPayload(AuthOpcode opcode, long senderId, byte[] publicKeyBytes, byte[] signature) {
    public DecodedEcdsaPayload {
      ValidationUtils.requireAllNonNull(named("opcode", opcode), named("publicKeyBytes", publicKeyBytes), named("signature", signature));
      ValidationUtils.requireExactInt(publicKeyBytes.length, EPHEMERAL_PUBLIC_KEY_BYTES, "publicKeyBytes.length");
      ValidationUtils.requirePositiveInt(signature.length, "signature.length");
    }
  }

  public static byte[] encodeEcdsa(AuthOpcode opcode, long senderId, PublicKey publicKey, PrivateKey privateKey) throws Exception {
    ValidationUtils.requireAllNonNull(named("opcode", opcode), named("publicKey", publicKey), named("privateKey", privateKey));
    byte[] publicKeyBytes = ValidationUtils.requirePresent(publicKey.getEncoded(), "publicKey must provide an encoded form");
    ValidationUtils.requireExactInt(publicKeyBytes.length, EPHEMERAL_PUBLIC_KEY_BYTES, "publicKeyBytes.length");

    byte[] signedData = buildEcdsaMessage(opcode, senderId, publicKeyBytes);
    byte[] signature = CryptoUtil.signEcdsa(signedData, privateKey);

    return appendOpcodeAndSignature(opcode, senderId, publicKeyBytes, signature);
  }

  public static DecodedEcdsaPayload decodeEcdsa(byte[] bytes) {
    ValidationUtils.requireNonNull(bytes, "bytes");
    int minLength = OPCODE_BYTES + Long.BYTES + EPHEMERAL_PUBLIC_KEY_BYTES + 1;
    ValidationUtils.requireAtLeastInt(bytes.length, minLength, "bytes length", Integer.toString(minLength));
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    AuthOpcode opcode = AuthOpcode.fromCode(buffer.get());
    validateHandshakeOpcode(opcode);
    long senderId = buffer.getLong();

    byte[] publicKeyBytes = new byte[EPHEMERAL_PUBLIC_KEY_BYTES];
    buffer.get(publicKeyBytes);

    byte[] signature = new byte[bytes.length - OPCODE_BYTES - Long.BYTES - EPHEMERAL_PUBLIC_KEY_BYTES];
    buffer.get(signature);

    return new DecodedEcdsaPayload(opcode, senderId, publicKeyBytes, signature);
  }

  public static boolean verifyEcdsa(byte[] bytes, PublicKey publicKey) throws Exception {
    ValidationUtils.requireNonNull(publicKey, "publicKey");

    DecodedEcdsaPayload decoded = decodeEcdsa(bytes);
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
    int minLength = OPCODE_BYTES + Long.BYTES + HMAC_BYTES;
    ValidationUtils.requireAtLeastInt(bytes.length, minLength, "bytes length", Integer.toString(minLength));
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    AuthOpcode opcode = AuthOpcode.fromCode(buffer.get());
    validateSecureDataOpcode(opcode);

    int payloadLength = bytes.length - OPCODE_BYTES - Long.BYTES - HMAC_BYTES;
    byte[] payload = new byte[payloadLength];

    buffer.get(payload);
    long nonce = buffer.getLong();
    byte[] hmac = new byte[HMAC_BYTES];
    buffer.get(hmac);

    return new DecodedHmacPayload(opcode, payload, nonce, hmac);
  }

  public static boolean verifyHmac(byte[] bytes, SecretKey key) throws Exception {
    ValidationUtils.requireNonNull(key, "key");

    DecodedHmacPayload decoded = decodeHmac(bytes);
    return CryptoUtil.verifyHmacWithNonce(buildMessageWithOpcode(decoded.opcode(), decoded.payload()), decoded.hmac(), decoded.nonce(), key);
  }

  private static byte[] appendOpcodeAndSignature(AuthOpcode opcode, long senderId, byte[] publicKeyBytes, byte[] signature) {
    ValidationUtils.requireAllNonNull(named("publicKeyBytes", publicKeyBytes), named("signature", signature));
    ValidationUtils.requireExactInt(publicKeyBytes.length, EPHEMERAL_PUBLIC_KEY_BYTES, "publicKeyBytes.length");
    ValidationUtils.requirePositiveInt(signature.length, "signature.length");

    ByteBuffer buffer = ByteBuffer.allocate(OPCODE_BYTES + Long.BYTES + publicKeyBytes.length + signature.length);
    buffer.put(opcode.code());
    buffer.putLong(senderId);
    buffer.put(publicKeyBytes);
    buffer.put(signature);

    return buffer.array();
  }

  private static byte[] appendOpcodeNonceAndHmac(AuthOpcode opcode, byte[] payload, long nonce, byte[] hmac) {
    ValidationUtils.requireExactInt(hmac.length, HMAC_BYTES, "hmac length");

    ByteBuffer buffer = ByteBuffer.allocate(OPCODE_BYTES + payload.length + Long.BYTES + HMAC_BYTES);
    buffer.put(opcode.code());
    buffer.put(payload);
    buffer.putLong(nonce);
    buffer.put(hmac);

    return buffer.array();
  }

  private static byte[] buildMessageWithOpcode(AuthOpcode opcode, byte[] payload) {
    ByteBuffer buffer = ByteBuffer.allocate(OPCODE_BYTES + payload.length);
    buffer.put(opcode.code());
    buffer.put(payload);

    return buffer.array();
  }

  private static byte[] buildEcdsaMessage(AuthOpcode opcode, long senderId, byte[] publicKeyBytes) {
    ByteBuffer buffer = ByteBuffer.allocate(OPCODE_BYTES + Long.BYTES + publicKeyBytes.length);
    buffer.put(opcode.code());
    buffer.putLong(senderId);
    buffer.put(publicKeyBytes);

    return buffer.array();
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
