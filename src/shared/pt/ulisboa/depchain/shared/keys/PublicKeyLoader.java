package pt.ulisboa.depchain.shared.keys;

import java.nio.file.Path;
import java.security.PublicKey;
import java.util.LinkedHashMap;
import java.util.Map;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class PublicKeyLoader {
  private PublicKeyLoader() {
  }

  public static Map<Long, PublicKey> loadStaticPublicKeys(ConfigParser config) throws Exception {
    ValidationUtils.requireNonNull(config, "config");

    Map<Long, PublicKey> publicKeyBySenderId = new LinkedHashMap<>();
    for (ConfigParser.ReplicaSection replica : config.replicas()) {
      Path publicKeyPath = Path.of(replica.publicKeyPath());
      PublicKey publicKey = KeyMaterialSupport.loadPublicKey(publicKeyPath);
      publicKeyBySenderId.put(replica.senderId(), publicKey);
    }

    Path clientPublicKeyPath = Path.of(config.client().publicKeyPath());
    PublicKey clientPublicKey = KeyMaterialSupport.loadPublicKey(clientPublicKeyPath);
    publicKeyBySenderId.put(config.client().senderId(), clientPublicKey);

    return Map.copyOf(publicKeyBySenderId);
  }

  public static PublicKey decodePublicKey(byte[] bytes) throws Exception {
    return KeyMaterialSupport.decodePublicKey(bytes);
  }
}
