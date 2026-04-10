package pt.ulisboa.depchain.tool.keymaterial;

import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.PublicKey;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.crypto.CryptoUtil;
import pt.ulisboa.depchain.shared.crypto.KeyUtil;
import pt.ulisboa.depchain.shared.crypto.ThresholdCryptoUtil;
import pt.ulisboa.depchain.shared.crypto.ThresholdCryptoUtil.ThresholdConfig;
import pt.ulisboa.depchain.shared.crypto.key.PublicKeyLoader;
import pt.ulisboa.depchain.shared.validation.ValidationUtils;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public final class KeyMaterialApplication {
  private static final Logger logger = LoggerFactory.getLogger(KeyMaterialApplication.class);
  private static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

  private KeyMaterialApplication() {
  }

  public static void main(String[] args) {
    int exitCode = new CommandLine(new KeyMaterialCommand()).execute(args);
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

  private static void generateKeyMaterial(Path configPath) throws Exception {
    ConfigParser config = ConfigParser.load(configPath);

    int n = config.system().n();
    int t = n - config.system().f();

    List<ConfigParser.ReplicaSection> replicas = config.replicas();

    for (ConfigParser.ReplicaSection replica : replicas) {
      writeReplicaKeys(replica);
    }

    ThresholdConfig thresholdConfig = ThresholdCryptoUtil.createThresholdConfig(n, t);
    for (int i = 0; i < replicas.size(); i++) {
      ConfigParser.ReplicaSection replica = replicas.get(i);
      writeReplicaThresholdMaterial(replica, thresholdConfig, i);
    }

    for (ConfigParser.ClientSection client : config.clients()) {
      writeClientKeys(client);
    }

    writeAddresses(configPath, config);
    logger.info("Generated individual keys for {} replicas, threshold material for {} replicas, {} clients, and wrote {}", replicas.size(), replicas.size(), config.clients()
        .size(), addressesPathFor(configPath));
  }

  @Command(name = "populate", mixinStandardHelpOptions = true, description = "Generate replica, threshold, and client key material.")
  private static final class KeyMaterialCommand implements Callable<Integer> {
    @Option(names = {"-c", "--config"}, description = "Path to the cluster configuration file.")
    private Path configOption;

    @Parameters(index = "0", arity = "0..1", defaultValue = "config/config.yaml", hidden = true, description = "Path to the cluster configuration file.")
    private Path configParameter;

    @Override
    public Integer call() throws Exception {
      Path configPath = configOption != null ? configOption : configParameter;
      ValidationUtils.requireNonNull(configPath, "configPath");
      generateKeyMaterial(configPath);
      return 0;
    }
  }

  private static void writeReplicaKeys(ConfigParser.ReplicaSection replica) throws Exception {
    ValidationUtils.requireNonNull(replica, "replica");

    Path publicKeyPath = replica.publicKeyPath();
    Path privateKeyPath = replica.privateKeyPath();

    createParentDirectories(publicKeyPath);
    createParentDirectories(privateKeyPath);

    if (Files.exists(publicKeyPath) && Files.exists(privateKeyPath)) {
      logger.info("Replica keys already exist for {} -> {}", replica.id(), publicKeyPath);
      return;
    }

    KeyPair keyPair = CryptoUtil.createEcKeyPair();

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

    if (Files.exists(thresholdPublicKeyPath) && Files.exists(thresholdPrivateSharePath)) {
      logger.info("Threshold material already exists for {} -> public: {}, share: {}", replica.id(), thresholdPublicKeyPath, thresholdPrivateSharePath);
      return;
    }

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

    Path publicKeyPath = client.publicKeyPath();
    Path privateKeyPath = client.privateKeyPath();

    createParentDirectories(publicKeyPath);
    createParentDirectories(privateKeyPath);

    if (Files.exists(publicKeyPath) && Files.exists(privateKeyPath)) {
      logger.info("Client keys already exist for {} -> {}", client.id(), publicKeyPath);
      return;
    }

    KeyPair keyPair = CryptoUtil.createEcKeyPair();

    Files.writeString(publicKeyPath, KeyUtil.encodePem("PUBLIC KEY", keyPair.getPublic().getEncoded()));
    Files.writeString(privateKeyPath, KeyUtil.encodePem("PRIVATE KEY", keyPair.getPrivate().getEncoded()));

    logger.info("Generated keys for {} -> {}", client.id(), publicKeyPath);
  }

  private static void writeAddresses(Path configPath, ConfigParser config) throws Exception {
    Map<String, String> clientAddresses = new LinkedHashMap<>();
    for (ConfigParser.ClientSection client : config.clients()) {
      PublicKey publicKey = PublicKeyLoader.loadClientPublicKey(config, client.senderId());
      clientAddresses.put(client.id(), CryptoUtil.deriveAddressHex(publicKey));
    }

    Map<Long, PublicKey> replicaPublicKeys = PublicKeyLoader.loadReplicaPublicKeys(config);
    Map<String, String> replicaAddresses = new LinkedHashMap<>();
    for (ConfigParser.ReplicaSection replica : config.replicas()) {
      replicaAddresses.put(replica.id(), CryptoUtil.deriveAddressHex(replicaPublicKeys.get(replica.senderId())));
    }

    Path addressesPath = addressesPathFor(configPath);
    Path parent = addressesPath.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }

    try (OutputStream output = Files.newOutputStream(addressesPath)) {
      JSON.writeValue(output, new AddressBook(clientAddresses, replicaAddresses));
    }
  }

  private static Path addressesPathFor(Path configPath) {
    Path parent = configPath.toAbsolutePath().normalize().getParent();
    if (parent == null) {
      throw new IllegalArgumentException("configPath must have a parent directory");
    }
    return parent.resolve("addresses.json");
  }

  private static void createParentDirectories(Path path) throws Exception {
    Path parent = path.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
  }

  private record AddressBook(Map<String, String> clients, Map<String, String> replicas) {
  }
}
