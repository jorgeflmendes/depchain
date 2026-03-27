package pt.ulisboa.depchain.shared.crypto;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.interfaces.ECPublicKey;
import java.util.Arrays;
import java.util.HexFormat;

import javax.crypto.KeyAgreement;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.bouncycastle.jcajce.provider.digest.Keccak;

public class CryptoUtil {
  private static final int EC_KEY_SIZE_BITS = 256;
  private static final int HMAC_SHA_256_KEY_BYTES = 32;
  private static final HexFormat HEX_FORMAT = HexFormat.of();
  private static final SecretKeySpec ZERO_HMAC_SHA_256_KEY = new SecretKeySpec(new byte[HMAC_SHA_256_KEY_BYTES], "HmacSHA256");
  private static final ThreadLocal<KeyPairGenerator> EC_KEY_PAIR_GENERATOR = ThreadLocal.withInitial(CryptoUtil::newEcKeyPairGenerator);
  private static final ThreadLocal<KeyAgreement> ECDH_KEY_AGREEMENT = ThreadLocal.withInitial(() -> newKeyAgreement("ECDH"));
  private static final ThreadLocal<Mac> HMAC_SHA_256 = ThreadLocal.withInitial(() -> newMac("HmacSHA256"));
  private static final ThreadLocal<MessageDigest> SHA_256 = ThreadLocal.withInitial(() -> newMessageDigest("SHA-256"));
  private static final ThreadLocal<Keccak.Digest256> KECCAK_256 = ThreadLocal.withInitial(Keccak.Digest256::new);

  public record KeyContext(String label, String step) {
    public byte[] toHkdfContext() {
      if (label == null) {
        throw new IllegalArgumentException("label cannot be null");
      }
      if (step == null) {
        throw new IllegalArgumentException("step cannot be null");
      }

      byte[] labelBytes = label.getBytes(StandardCharsets.UTF_8);
      byte[] stepBytes = step.getBytes(StandardCharsets.UTF_8);
      ByteBuffer buffer = ByteBuffer.allocate((2 * Integer.BYTES) + labelBytes.length + stepBytes.length);
      buffer.putInt(labelBytes.length);
      buffer.put(labelBytes);
      buffer.putInt(stepBytes.length);
      buffer.put(stepBytes);
      return buffer.array();
    }
  }

  public static KeyPair createEcKeyPair() throws Exception {
    return EC_KEY_PAIR_GENERATOR.get().generateKeyPair();
  }

  public static SecretKey deriveCommonKey(PrivateKey privateKey, PublicKey publicKey, KeyContext keyContext) throws Exception {
    KeyAgreement agreement = ECDH_KEY_AGREEMENT.get();
    agreement.init(privateKey);
    agreement.doPhase(publicKey, true);
    byte[] sharedSecret = agreement.generateSecret();
    Mac mac = HMAC_SHA_256.get();
    mac.init(ZERO_HMAC_SHA_256_KEY);
    byte[] prk = mac.doFinal(sharedSecret);
    mac.init(new SecretKeySpec(prk, "HmacSHA256"));
    if (keyContext != null) {
      mac.update(keyContext.toHkdfContext());
    }
    mac.update((byte) 0x01);
    byte[] okm = mac.doFinal();
    Arrays.fill(sharedSecret, (byte) 0);
    Arrays.fill(prk, (byte) 0);
    SecretKeySpec derivedKey = new SecretKeySpec(okm, 0, HMAC_SHA_256_KEY_BYTES, "HmacSHA256");
    Arrays.fill(okm, (byte) 0);
    return derivedKey;
  }

  public static byte[] signHmacWithNonce(byte[] message, SecretKey key, long nonce) throws Exception {
    Mac mac = HMAC_SHA_256.get();
    mac.init(key);
    mac.update(message);
    updateLong(mac, nonce);
    return mac.doFinal();
  }

  public static boolean verifyHmacWithNonce(byte[] message, byte[] hmac, long nonce, SecretKey key) throws Exception {
    Mac mac = HMAC_SHA_256.get();
    mac.init(key);
    mac.update(message);
    updateLong(mac, nonce);
    byte[] expectedHmac = mac.doFinal();

    return MessageDigest.isEqual(expectedHmac, hmac);
  }

  public static byte[] signEcdsa(byte[] data, PrivateKey secretKey) throws Exception {
    Signature ecdsaSign = newSignature("SHA256withECDSA");
    ecdsaSign.initSign(secretKey);
    ecdsaSign.update(data);
    return ecdsaSign.sign();
  }

  public static boolean verifyEcdsa(byte[] data, byte[] signature, PublicKey publicKey) throws Exception {
    Signature ecdsaVerify = newSignature("SHA256withECDSA");
    ecdsaVerify.initVerify(publicKey);
    ecdsaVerify.update(data);
    return ecdsaVerify.verify(signature);
  }

  public static String deriveAddressHex(PublicKey publicKey) {
    if (!(publicKey instanceof ECPublicKey ecPublicKey)) {
      throw new IllegalArgumentException("publicKey must be an EC public key");
    }

    byte[] rawPublicKey = rawEcPublicKey(ecPublicKey);
    byte[] hash = KECCAK_256.get().digest(rawPublicKey);
    return HEX_FORMAT.formatHex(hash, hash.length - 20, hash.length);
  }

  public static String sha256Hex(byte[] value) {
    if (value == null) {
      throw new IllegalArgumentException("value cannot be null");
    }
    byte[] hash = SHA_256.get().digest(value);
    return HEX_FORMAT.formatHex(hash);
  }

  private static byte[] rawEcPublicKey(ECPublicKey publicKey) {
    int coordinateLength = (publicKey.getParams().getCurve().getField().getFieldSize() + 7) / 8;
    byte[] x = toFixedLength(publicKey.getW().getAffineX().toByteArray(), coordinateLength);
    byte[] y = toFixedLength(publicKey.getW().getAffineY().toByteArray(), coordinateLength);
    byte[] raw = new byte[coordinateLength * 2];
    System.arraycopy(x, 0, raw, 0, coordinateLength);
    System.arraycopy(y, 0, raw, coordinateLength, coordinateLength);
    return raw;
  }

  private static byte[] toFixedLength(byte[] value, int expectedLength) {
    if (value.length == expectedLength) {
      return value;
    }
    if (value.length == expectedLength + 1 && value[0] == 0) {
      return Arrays.copyOfRange(value, 1, value.length);
    }
    if (value.length > expectedLength) {
      throw new IllegalArgumentException("EC coordinate exceeds expected length");
    }

    byte[] padded = new byte[expectedLength];
    System.arraycopy(value, 0, padded, expectedLength - value.length, value.length);
    return padded;
  }

  private static Mac newMac(String algorithm) {
    try {
      return Mac.getInstance(algorithm);
    } catch (Exception exception) {
      throw new IllegalStateException(algorithm + " is not available", exception);
    }
  }

  private static KeyPairGenerator newEcKeyPairGenerator() {
    try {
      KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
      generator.initialize(EC_KEY_SIZE_BITS);
      return generator;
    } catch (Exception exception) {
      throw new IllegalStateException("EC key pair generation is not available", exception);
    }
  }

  private static KeyAgreement newKeyAgreement(String algorithm) {
    try {
      return KeyAgreement.getInstance(algorithm);
    } catch (Exception exception) {
      throw new IllegalStateException(algorithm + " is not available", exception);
    }
  }

  private static Signature newSignature(String algorithm) {
    try {
      return Signature.getInstance(algorithm);
    } catch (Exception exception) {
      throw new IllegalStateException(algorithm + " is not available", exception);
    }
  }

  private static MessageDigest newMessageDigest(String algorithm) {
    try {
      return MessageDigest.getInstance(algorithm);
    } catch (Exception exception) {
      throw new IllegalStateException(algorithm + " is not available", exception);
    }
  }

  private static void updateLong(Mac mac, long value) {
    mac.update((byte) (value >>> 56));
    mac.update((byte) (value >>> 48));
    mac.update((byte) (value >>> 40));
    mac.update((byte) (value >>> 32));
    mac.update((byte) (value >>> 24));
    mac.update((byte) (value >>> 16));
    mac.update((byte) (value >>> 8));
    mac.update((byte) value);
  }
}
