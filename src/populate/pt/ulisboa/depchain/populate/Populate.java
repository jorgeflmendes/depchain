package pt.ulisboa.depchain.populate;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;
import pt.ulisboa.depchain.shared.utils.KeyUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class Populate {
  private Populate() {
  }

  public static void main(String[] args) throws Exception {
    ValidationUtils.requireNonNull(args, "args");

    Path configPath;
    if (args.length == 0) {
      configPath = Path.of("config", "config.properties");
    } else if (args.length == 1) {
      configPath = Path.of(args[0]);
    } else {
      throw new IllegalArgumentException("Usage: Populate [configPath]");
    }

    ConfigParser config = ConfigParser.load(configPath);
    for (ConfigParser.ReplicaSection replica : config.replicas()) {
      writeReplicaKeys(replica);
    }
    writeClientKeys(config.client());

    System.out.println("Generated keys for " + config.replicas().size() + " replicas and 1 client");
  }

  private static void writeReplicaKeys(ConfigParser.ReplicaSection replica) throws Exception {
    ValidationUtils.requireNonNull(replica, "replica");

    KeyPair keyPair = CryptoUtil.newECKeyPair();
    Path publicKeyPath = Path.of(replica.publicKeyPath());
    Path privateKeyPath = Path.of(replica.privateKeyPath());

    Files.createDirectories(publicKeyPath.getParent());
    Files.createDirectories(privateKeyPath.getParent());

    Files.writeString(publicKeyPath, KeyUtil.encodePem("PUBLIC KEY", keyPair.getPublic().getEncoded()));
    Files.writeString(privateKeyPath, KeyUtil.encodePem("PRIVATE KEY", keyPair.getPrivate().getEncoded()));

    System.out.println("Generated keys for " + replica.id() + " -> " + publicKeyPath);
  }

  private static void writeClientKeys(ConfigParser.ClientSection client) throws Exception {
    ValidationUtils.requireNonNull(client, "client");

    KeyPair keyPair = CryptoUtil.newECKeyPair();
    Path publicKeyPath = Path.of(client.publicKeyPath());
    Path privateKeyPath = Path.of(client.privateKeyPath());

    Files.createDirectories(publicKeyPath.getParent());
    Files.createDirectories(privateKeyPath.getParent());

    Files.writeString(publicKeyPath, KeyUtil.encodePem("PUBLIC KEY", keyPair.getPublic().getEncoded()));
    Files.writeString(privateKeyPath, KeyUtil.encodePem("PRIVATE KEY", keyPair.getPrivate().getEncoded()));

    System.out.println("Generated keys for " + client.id() + " -> " + publicKeyPath);
  }
}
