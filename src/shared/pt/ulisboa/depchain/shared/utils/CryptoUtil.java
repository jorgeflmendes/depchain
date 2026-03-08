package pt.ulisboa.depchain.shared.utils;

import javax.crypto.KeyAgreement;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.nio.ByteBuffer;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Arrays;

public class CryptoUtil {
  public record KeyContext(String label, String step) {
    public byte[] toHkdfContext() {
      String value = label + "|" + step;
      return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
  }

  public static KeyPair newECKeyPair() throws Exception {
    KeyPairGenerator gen = KeyPairGenerator.getInstance("EC");
    gen.initialize(256);
    return gen.generateKeyPair();
  }

  public static SecretKey deriveCommonKey(PrivateKey privateKey, PublicKey publicKey, KeyContext keyContext) throws Exception {
    // ECDH key agreement
    KeyAgreement agreement = KeyAgreement.getInstance("ECDH");
    agreement.init(privateKey);
    agreement.doPhase(publicKey, true);

    // Derive a shared secret
    byte[] sharedSecret = agreement.generateSecret();

    // Use HKDF to derive a symmetric key from the shared secret
    Mac mac = Mac.getInstance("HmacSHA256");

    // HKDF-Extract
    byte[] effectiveSalt = new byte[32];
    mac.init(new SecretKeySpec(effectiveSalt, "HmacSHA256"));
    byte[] prk = mac.doFinal(sharedSecret);

    // HKDF-Expand
    mac.init(new SecretKeySpec(prk, "HmacSHA256"));
    if (keyContext != null) {
      mac.update(keyContext.toHkdfContext());
    }
    mac.update((byte) 0x01); // RFC requires this
    byte[] okm = mac.doFinal();

    // Cleanup
    Arrays.fill(sharedSecret, (byte) 0);
    Arrays.fill(prk, (byte) 0);

    return new SecretKeySpec(Arrays.copyOf(okm, 32), "HmacSHA256");
  }

  public static byte[] signHmacWithNonce(byte[] message, SecretKey key, long nonce) throws Exception {
    // [Message + Nonce]
    ByteBuffer buffer = ByteBuffer.allocate(message.length + Long.BYTES);
    buffer.put(message);
    buffer.putLong(nonce);
    byte[] dataToSign = buffer.array();

    // HMAC
    Mac mac = Mac.getInstance("HmacSHA256");
    mac.init(key);

    return mac.doFinal(dataToSign);
  }

  public static boolean verifyHmacWithNonce(byte[] message, byte[] hmac, long nonce, SecretKey key) throws Exception {
    // [Message + Nonce]
    ByteBuffer buffer = ByteBuffer.allocate(message.length + Long.BYTES);
    buffer.put(message);
    buffer.putLong(nonce);
    byte[] dataToVerify = buffer.array();

    // HMAC
    Mac mac = Mac.getInstance("HmacSHA256");
    mac.init(key);
    byte[] expectedHmac = mac.doFinal(dataToVerify);

    return MessageDigest.isEqual(expectedHmac, hmac);
  }

  public static byte[] signEcdsa(byte[] data, PrivateKey secretKey) throws Exception {
    Signature ecdsaSign = Signature.getInstance("SHA256withECDSA");
    ecdsaSign.initSign(secretKey);
    ecdsaSign.update(data);

    return ecdsaSign.sign();
  }

  public static boolean verifyEcdsa(byte[] data, byte[] signature, PublicKey publicKey) throws Exception {
    Signature ecdsaVerify = Signature.getInstance("SHA256withECDSA");
    ecdsaVerify.initVerify(publicKey);
    ecdsaVerify.update(data);

    return ecdsaVerify.verify(signature);
  }
}
