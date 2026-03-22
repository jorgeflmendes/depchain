package pt.ulisboa.depchain.populate;

import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.util.List;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;
import pt.ulisboa.depchain.shared.utils.KeyUtil;
import pt.ulisboa.depchain.shared.utils.ThresholdCryptoUtil;
import pt.ulisboa.depchain.shared.utils.ThresholdCryptoUtil.ThresholdConfig;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public final class Populate {
  private static final Logger logger = LoggerFactory.getLogger(Populate.class);

  private Populate() {
  }

  public static void main(String[] args) {
    int exitCode = new CommandLine(new PopulateCommand()).execute(args);
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

  private static void runPopulate(Path configPath) throws Exception {
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

    for (ConfigParser.ClientSection client : config.clients()) {
      writeClientKeys(client);
    }

    logger.info("Generated individual keys for {} replicas, threshold material for {} replicas, and {} clients", replicas.size(), replicas.size(), config.clients().size());
  }

  @Command(name = "populate", mixinStandardHelpOptions = true, description = "Generate replica, threshold, and client key material.")
  private static final class PopulateCommand implements Callable<Integer> {
    @Option(names = {"-c", "--config"}, description = "Path to the cluster configuration file.")
    private Path configOption;

    @Parameters(index = "0", arity = "0..1", defaultValue = "config/config.yaml", hidden = true, description = "Path to the cluster configuration file.")
    private Path configParameter;

    @Override
    public Integer call() throws Exception {
      Path configPath = configOption != null ? configOption : configParameter;
      ValidationUtils.requireNonNull(configPath, "configPath");
      runPopulate(configPath);
      return 0;
    }
  }

  private static void writeReplicaKeys(ConfigParser.ReplicaSection replica) throws Exception {
    ValidationUtils.requireNonNull(replica, "replica");

    KeyPair keyPair = CryptoUtil.newECKeyPair();
    Path publicKeyPath = replica.publicKeyPath();
    Path privateKeyPath = replica.privateKeyPath();

    createParentDirectories(publicKeyPath);
    createParentDirectories(privateKeyPath);

    Files.writeString(publicKeyPath, KeyUtil.encodePem("PUBLIC KEY", keyPair.getPublic().getEncoded()));
    Files.writeString(privateKeyPath, KeyUtil.encodePem("PRIVATE KEY", keyPair.getPrivate().getEncoded()));

    logger.info("Generated individual keys for {} -> {}", replica.id(), publicKeyPath);
  }

  private static void writeReplicaThresholdMaterial(ConfigParser.ReplicaSection replica, ThresholdConfig thresholdConfig, int replicaIndex) throws Exception {

    ValidationUtils.requireNonNull(replica, "replica");
    ValidationUtils.requireNonNull(thresholdConfig, "thresholdConfig");
    ValidationUtils.requireNonNegativeInt(replicaIndex, "replicaIndex");

    Path thresholdPublicKeyPath = replica.thresholdPublicKeyPath();
    Path thresholdPrivateSharePath = replica.thresholdPrivateSharePath();

    createParentDirectories(thresholdPublicKeyPath);
    createParentDirectories(thresholdPrivateSharePath);

    try (ObjectOutputStream out = new ObjectOutputStream(Files.newOutputStream(thresholdPublicKeyPath))) {
      out.writeObject(thresholdConfig.publicKey());
    }

    try (ObjectOutputStream out = new ObjectOutputStream(Files.newOutputStream(thresholdPrivateSharePath))) {
      out.writeObject(thresholdConfig.privateShare(replicaIndex));
    }

    logger.info("Generated threshold material for {} -> public: {}, share: {}", replica.id(), thresholdPublicKeyPath, thresholdPrivateSharePath);
  }

  private static void writeClientKeys(ConfigParser.ClientSection client) throws Exception {
    ValidationUtils.requireNonNull(client, "client");

    KeyPair keyPair = CryptoUtil.newECKeyPair();
    Path publicKeyPath = client.publicKeyPath();
    Path privateKeyPath = client.privateKeyPath();

    createParentDirectories(publicKeyPath);
    createParentDirectories(privateKeyPath);

    Files.writeString(publicKeyPath, KeyUtil.encodePem("PUBLIC KEY", keyPair.getPublic().getEncoded()));
    Files.writeString(privateKeyPath, KeyUtil.encodePem("PRIVATE KEY", keyPair.getPrivate().getEncoded()));

    logger.info("Generated keys for {} -> {}", client.id(), publicKeyPath);
  }

  private static void createParentDirectories(Path path) throws Exception {
    Path parent = path.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
  }
}
