package pt.ulisboa.depchain.shared.crypto.key;

import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import com.weavechain.curve25519.Scalar;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.validation.ValidationUtils;

public final class ThresholdKeyLoader {
  private ThresholdKeyLoader() {
  }

  public static ReplicaThresholdKeyMaterial loadReplicaThresholdKeyMaterial(ConfigParser config, long senderId) throws Exception {
    ValidationUtils.requireNonNull(config, "config");
    ValidationUtils.requireNonNegativeLong(senderId, "senderId");

    ConfigParser.ReplicaSection replica = config.requireReplicaBySenderId(senderId);
    byte[] publicKey = readSerialized(replica.thresholdPublicKeyPath(), byte[].class);
    Scalar privateShare = readSerialized(replica.thresholdPrivateSharePath(), Scalar.class);
    return new ReplicaThresholdKeyMaterial(publicKey, privateShare);
  }

  public static byte[] loadReplicaThresholdPublicKey(ConfigParser config, long senderId) throws Exception {
    return loadReplicaThresholdKeyMaterial(config, senderId).publicKey();
  }

  public static Scalar loadReplicaThresholdPrivateShare(ConfigParser config, long senderId) throws Exception {
    return loadReplicaThresholdKeyMaterial(config, senderId).privateShare();
  }

  public record ReplicaThresholdKeyMaterial(byte[] publicKey, Scalar privateShare) {
    public ReplicaThresholdKeyMaterial {
      ValidationUtils.requireNonNull(publicKey, "publicKey");
      ValidationUtils.requireNonNull(privateShare, "privateShare");
      publicKey = publicKey.clone();
    }

    @Override
    public byte[] publicKey() {
      return publicKey.clone();
    }
  }

  private static <T> T readSerialized(Path path, Class<T> type) throws Exception {
    ValidationUtils.requireNonNull(path, "path");
    ValidationUtils.requireNonNull(type, "type");

    try (var input = Files.newInputStream(path); var in = new ObjectInputStream(input)) {
      Object value = in.readObject();
      if (!type.isInstance(value)) {
        String actualType;
        if (value == null) {
          actualType = "null";
        } else {
          actualType = value.getClass().getName();
        }
        throw new IllegalArgumentException("Unexpected key material type in " + path + ": " + actualType);
      }
      return type.cast(value);
    }
  }
}
