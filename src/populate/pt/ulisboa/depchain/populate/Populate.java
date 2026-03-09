package pt.ulisboa.depchain.populate;

import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.util.List;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;
import pt.ulisboa.depchain.shared.utils.KeyUtil;
import pt.ulisboa.depchain.shared.utils.ThresholdCryptoUtil;
import pt.ulisboa.depchain.shared.utils.ThresholdCryptoUtil.ThresholdConfig;
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

    int n = config.system().n();
    int t = n - config.system().f();

    List<ConfigParser.ReplicaSection> replicas = config.replicas();

    for (ConfigParser.ReplicaSection replica : replicas) {
      writeReplicaKeys(replica);
    }

    ThresholdConfig thresholdConfig = ThresholdCryptoUtil.newThresholdConfig(n, t);
    for (int i = 0; i < replicas.size(); i++) {
      ConfigParser.ReplicaSection replica = replicas.get(i);
      writeReplicaThresholdMaterial(replica, thresholdConfig, i);
    }

    writeClientKeys(config.client());

    System.out.println("Generated individual keys for " + replicas.size() + " replicas, threshold material for " + replicas.size() + " replicas, and 1 client");
  }

  private static void writeReplicaKeys(ConfigParser.ReplicaSection replica) throws Exception {
    ValidationUtils.requireNonNull(replica, "replica");

    KeyPair keyPair = CryptoUtil.newECKeyPair();
    Path publicKeyPath = Path.of(replica.publicKeyPath());
    Path privateKeyPath = Path.of(replica.privateKeyPath());

    createParentDirectories(publicKeyPath);
    createParentDirectories(privateKeyPath);

    Files.writeString(publicKeyPath, KeyUtil.encodePem("PUBLIC KEY", keyPair.getPublic().getEncoded()));
    Files.writeString(privateKeyPath, KeyUtil.encodePem("PRIVATE KEY", keyPair.getPrivate().getEncoded()));

    System.out.println("Generated individual keys for " + replica.id() + " -> " + publicKeyPath);
  }

  private static void writeReplicaThresholdMaterial(ConfigParser.ReplicaSection replica, ThresholdConfig thresholdConfig, int replicaIndex) throws Exception {

    ValidationUtils.requireNonNull(replica, "replica");
    ValidationUtils.requireNonNull(thresholdConfig, "thresholdConfig");
    ValidationUtils.requireNonNegativeInt(replicaIndex, "replicaIndex");

    Path thresholdPublicKeyPath = Path.of(replica.thresholdPublicKeyPath());
    Path thresholdPrivateSharePath = Path.of(replica.thresholdPrivateSharePath());

    createParentDirectories(thresholdPublicKeyPath);
    createParentDirectories(thresholdPrivateSharePath);

    try (ObjectOutputStream out = new ObjectOutputStream(Files.newOutputStream(thresholdPublicKeyPath))) {
      out.writeObject(thresholdConfig.publicKey());
    }

    try (ObjectOutputStream out = new ObjectOutputStream(Files.newOutputStream(thresholdPrivateSharePath))) {
      out.writeObject(thresholdConfig.privateShare(replicaIndex));
    }

    System.out.println("Generated threshold material for " + replica.id() + " -> public: " + thresholdPublicKeyPath + ", share: " + thresholdPrivateSharePath);
  }

  private static void writeClientKeys(ConfigParser.ClientSection client) throws Exception {
    ValidationUtils.requireNonNull(client, "client");

    KeyPair keyPair = CryptoUtil.newECKeyPair();
    Path publicKeyPath = Path.of(client.publicKeyPath());
    Path privateKeyPath = Path.of(client.privateKeyPath());

    createParentDirectories(publicKeyPath);
    createParentDirectories(privateKeyPath);

    Files.writeString(publicKeyPath, KeyUtil.encodePem("PUBLIC KEY", keyPair.getPublic().getEncoded()));
    Files.writeString(privateKeyPath, KeyUtil.encodePem("PRIVATE KEY", keyPair.getPrivate().getEncoded()));

    System.out.println("Generated keys for " + client.id() + " -> " + publicKeyPath);
  }

  private static void createParentDirectories(Path path) throws Exception {
    Path parent = path.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
  }
}
