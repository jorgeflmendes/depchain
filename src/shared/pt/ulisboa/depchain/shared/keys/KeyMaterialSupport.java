package pt.ulisboa.depchain.shared.keys;

import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.utils.KeyUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class KeyMaterialSupport {
  private static final String KEY_ALGORITHM = "EC";
  private static final String PEM_BEGIN_PRIVATE_KEY = "-----BEGIN PRIVATE KEY-----";
  private static final String PEM_END_PRIVATE_KEY = "-----END PRIVATE KEY-----";
  private static final String PEM_BEGIN_PUBLIC_KEY = "-----BEGIN PUBLIC KEY-----";
  private static final String PEM_END_PUBLIC_KEY = "-----END PUBLIC KEY-----";

  private KeyMaterialSupport() {
  }

  static ConfigParser.ReplicaSection requireReplica(ConfigParser config, long senderId) {
    ValidationUtils.requireNonNull(config, "config");
    ValidationUtils.requireNonNegativeLong(senderId, "senderId");

    return config.requireReplicaBySenderId(senderId);
  }

  static PrivateKey loadPrivateKey(Path path) throws Exception {
    return decodePrivateKey(readBytes(path));
  }

  static PublicKey loadPublicKey(Path path) throws Exception {
    return decodePublicKey(readBytes(path));
  }

  static PrivateKey decodePrivateKey(byte[] bytes) throws Exception {
    ValidationUtils.requireNonNull(bytes, "bytes");

    byte[] encodedKey = KeyUtil.decodePemIfNeeded(bytes, PEM_BEGIN_PRIVATE_KEY, PEM_END_PRIVATE_KEY);
    KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORITHM);
    return keyFactory.generatePrivate(new PKCS8EncodedKeySpec(encodedKey));
  }

  static PublicKey decodePublicKey(byte[] bytes) throws Exception {
    ValidationUtils.requireNonNull(bytes, "bytes");

    byte[] encodedKey = KeyUtil.decodePemIfNeeded(bytes, PEM_BEGIN_PUBLIC_KEY, PEM_END_PUBLIC_KEY);
    KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORITHM);
    return keyFactory.generatePublic(new X509EncodedKeySpec(encodedKey));
  }

  static <T> T readSerialized(Path path, Class<T> type) throws Exception {
    ValidationUtils.requireNonNull(path, "path");
    ValidationUtils.requireNonNull(type, "type");

    try (ObjectInputStream in = new ObjectInputStream(Files.newInputStream(path))) {
      Object value = in.readObject();
      if (!type.isInstance(value)) {
        String actualType = value == null ? "null" : value.getClass().getName();
        throw new IllegalArgumentException("Unexpected key material type in " + path + ": " + actualType);
      }
      return type.cast(value);
    }
  }

  private static byte[] readBytes(Path path) throws Exception {
    ValidationUtils.requireNonNull(path, "path");
    return Files.readAllBytes(path);
  }
}
