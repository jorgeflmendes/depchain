package pt.ulisboa.depchain.shared.keys;

import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.LinkedHashMap;
import java.util.Map;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.utils.KeyUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class PublicKeyLoader {
  private static final String PEM_BEGIN_PUBLIC_KEY = "-----BEGIN PUBLIC KEY-----";
  private static final String PEM_END_PUBLIC_KEY = "-----END PUBLIC KEY-----";

  public static PublicKey loadPublicKey(Path path) throws Exception {
    ValidationUtils.requireNonNull(path, "path");

    byte[] fileBytes = Files.readAllBytes(path);
    return decodePublicKey(fileBytes);
  }

  public static PublicKey decodePublicKey(byte[] bytes) throws Exception {
    ValidationUtils.requireNonNull(bytes, "bytes");

    byte[] encodedKey = KeyUtil.decodePemIfNeeded(bytes, PEM_BEGIN_PUBLIC_KEY, PEM_END_PUBLIC_KEY);
    KeyFactory keyFactory = KeyFactory.getInstance("EC");
    return keyFactory.generatePublic(new X509EncodedKeySpec(encodedKey));
  }

  public static Map<Long, PublicKey> loadStaticPublicKeys(ConfigParser config) throws Exception {
    ValidationUtils.requireNonNull(config, "config");

    Map<Long, PublicKey> publicKeyBySenderId = new LinkedHashMap<>();
    for (ConfigParser.ReplicaSection replica : config.replicas()) {
      Path publicKeyPath = Path.of(replica.publicKeyPath());
      PublicKey publicKey = loadPublicKey(publicKeyPath);
      publicKeyBySenderId.put(replica.senderId(), publicKey);
    }

    Path clientPublicKeyPath = Path.of(config.client().publicKeyPath());
    PublicKey clientPublicKey = loadPublicKey(clientPublicKeyPath);
    publicKeyBySenderId.put(config.client().senderId(), clientPublicKey);

    return Map.copyOf(publicKeyBySenderId);
  }

  public static byte[] loadThresholdPublicKey(Path path) throws Exception {
    ValidationUtils.requireNonNull(path, "path");

    try (ObjectInputStream in = new ObjectInputStream(Files.newInputStream(path))) {
      return (byte[]) in.readObject();
    }
  }

  public static byte[] loadReplicaThresholdPublicKey(ConfigParser config, long senderId) throws Exception {
    ValidationUtils.requireNonNull(config, "config");

    for (ConfigParser.ReplicaSection replica : config.replicas()) {
      if (replica.senderId() == senderId) {
        Path thresholdPublicKeyPath = Path.of(replica.thresholdPublicKeyPath());
        return loadThresholdPublicKey(thresholdPublicKeyPath);
      }
    }

    throw new IllegalArgumentException("Replica senderId '%s' not found in config".formatted(senderId));
  }

  public static Map<Long, byte[]> loadThresholdPublicKeys(ConfigParser config) throws Exception {
    ValidationUtils.requireNonNull(config, "config");

    Map<Long, byte[]> thresholdPublicKeyBySenderId = new LinkedHashMap<>();
    for (ConfigParser.ReplicaSection replica : config.replicas()) {
      Path thresholdPublicKeyPath = Path.of(replica.thresholdPublicKeyPath());
      byte[] thresholdPublicKey = loadThresholdPublicKey(thresholdPublicKeyPath);
      thresholdPublicKeyBySenderId.put(replica.senderId(), thresholdPublicKey);
    }

    return Map.copyOf(thresholdPublicKeyBySenderId);
  }
}