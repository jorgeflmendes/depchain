package pt.ulisboa.depchain.shared.keys;

import java.nio.file.Path;

import com.weavechain.curve25519.Scalar;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class ThresholdKeyLoader {
  private ThresholdKeyLoader() {
  }

  public static ReplicaThresholdKeyMaterial loadReplicaThresholdKeyMaterial(ConfigParser config, long senderId) throws Exception {
    ValidationUtils.requireNonNull(config, "config");

    ConfigParser.ReplicaSection replica = KeyMaterialSupport.requireReplica(config, senderId);
    byte[] publicKey = KeyMaterialSupport.readSerialized(Path.of(replica.thresholdPublicKeyPath()), byte[].class);
    Scalar privateShare = KeyMaterialSupport.readSerialized(Path.of(replica.thresholdPrivateSharePath()), Scalar.class);
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
}
