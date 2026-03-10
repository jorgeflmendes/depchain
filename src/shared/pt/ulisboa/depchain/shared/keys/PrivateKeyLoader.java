package pt.ulisboa.depchain.shared.keys;

import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;

import com.weavechain.curve25519.Scalar;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.utils.KeyUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class PrivateKeyLoader {
  private static final String PEM_BEGIN_PRIVATE_KEY = "-----BEGIN PRIVATE KEY-----";
  private static final String PEM_END_PRIVATE_KEY = "-----END PRIVATE KEY-----";

  public static PrivateKey loadReplicaPrivateKey(ConfigParser config, long senderId) throws Exception {
    ValidationUtils.requireNonNull(config, "config");

    for (ConfigParser.ReplicaSection replica : config.replicas()) {
      if (replica.senderId() == senderId) {
        Path privateKeyPath = Path.of(replica.privateKeyPath());
        return loadPrivateKey(privateKeyPath);
      }
    }

    throw new IllegalArgumentException("Replica senderId '%s' not found in config".formatted(senderId));
  }

  public static PrivateKey loadClientPrivateKey(ConfigParser config) throws Exception {
    ValidationUtils.requireNonNull(config, "config");

    Path privateKeyPath = Path.of(config.client().privateKeyPath());
    return loadPrivateKey(privateKeyPath);
  }

  public static Scalar loadReplicaThresholdPrivateShare(ConfigParser config, long senderId) throws Exception {
    ValidationUtils.requireNonNull(config, "config");

    for (ConfigParser.ReplicaSection replica : config.replicas()) {
      if (replica.senderId() == senderId) {
        Path thresholdPrivateSharePath = Path.of(replica.thresholdPrivateSharePath());
        return readThresholdPrivateShare(thresholdPrivateSharePath);
      }
    }

    throw new IllegalArgumentException("Replica senderId '%s' not found in config".formatted(senderId));
  }

  private static PrivateKey loadPrivateKey(Path path) throws Exception {
    ValidationUtils.requireNonNull(path, "path");

    byte[] fileBytes = Files.readAllBytes(path);
    return decodePrivateKey(fileBytes);
  }

  private static PrivateKey decodePrivateKey(byte[] bytes) throws Exception {
    ValidationUtils.requireNonNull(bytes, "bytes");

    byte[] encodedKey = KeyUtil.decodePemIfNeeded(bytes, PEM_BEGIN_PRIVATE_KEY, PEM_END_PRIVATE_KEY);
    KeyFactory keyFactory = KeyFactory.getInstance("EC");
    return keyFactory.generatePrivate(new PKCS8EncodedKeySpec(encodedKey));
  }

  private static Scalar readThresholdPrivateShare(Path path) throws Exception {
    ValidationUtils.requireNonNull(path, "path");

    try (ObjectInputStream in = new ObjectInputStream(Files.newInputStream(path))) {
      return (Scalar) in.readObject();
    }
  }
}
