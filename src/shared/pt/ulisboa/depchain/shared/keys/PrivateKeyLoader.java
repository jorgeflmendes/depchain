package pt.ulisboa.depchain.shared.keys;

import java.nio.file.Path;
import java.security.PrivateKey;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class PrivateKeyLoader {
  private PrivateKeyLoader() {
  }

  public static PrivateKey loadReplicaPrivateKey(ConfigParser config, long senderId) throws Exception {
    ValidationUtils.requireNonNull(config, "config");

    ConfigParser.ReplicaSection replica = KeyMaterialSupport.requireReplica(config, senderId);
    return KeyMaterialSupport.loadPrivateKey(Path.of(replica.privateKeyPath()));
  }

  public static PrivateKey loadClientPrivateKey(ConfigParser config) throws Exception {
    ValidationUtils.requireNonNull(config, "config");

    Path privateKeyPath = Path.of(config.client().privateKeyPath());
    return KeyMaterialSupport.loadPrivateKey(privateKeyPath);
  }

  public static PrivateKey decodePrivateKey(byte[] bytes) throws Exception {
    return KeyMaterialSupport.decodePrivateKey(bytes);
  }
}
