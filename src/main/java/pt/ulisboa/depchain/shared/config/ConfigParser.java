package pt.ulisboa.depchain.shared.config;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public record ConfigParser(SystemSection system, List<ReplicaSection> replicas, ClientSection client, TimeoutsSection timeouts, StorageSection storage, KeysSection keys) {
  private static final ObjectMapper YAML = YAMLMapper.builder().build();

  public ConfigParser {
    ValidationUtils
        .requireAllNonNull(named("system", system), named("replicas", replicas), named("client", client), named("timeouts", timeouts), named("storage", storage), named("keys", keys));
    replicas = List.copyOf(ValidationUtils.requireNonEmpty(replicas, "replicas"));
    validateConsistency(system, replicas, client);
  }

  public record SystemSection(int n, int f) {
    public SystemSection {
      ValidationUtils.requirePositiveInt(n, "system.n");
      ValidationUtils.requireNonNegativeInt(f, "system.f");
    }
  }

  public record ReplicaSection(String id, long senderId, String host, ReplicaPortsSection ports, ReplicaKeysSection keys) {
    public ReplicaSection {
      id = ValidationUtils.requireNonBlank(id, "replica.id");
      ValidationUtils.requireNonNegativeLong(senderId, "replica.senderId");
      host = ValidationUtils.requireNonBlank(host, "replica.host");
      ValidationUtils.requireNonNull(ports, "replica.ports");
      ValidationUtils.requireNonNull(keys, "replica.keys");
    }

    public int consensusPort() {
      return ports.consensus();
    }

    public int clientPort() {
      return ports.client();
    }

    public Path publicKeyPath() {
      return keys.publicKeyPath();
    }

    public Path privateKeyPath() {
      return keys.privateKeyPath();
    }

    public Path thresholdPublicKeyPath() {
      return keys.threshold().publicKeyPath();
    }

    public Path thresholdPrivateSharePath() {
      return keys.threshold().privateSharePath();
    }
  }

  public record ReplicaPortsSection(int consensus, int client) {
    public ReplicaPortsSection {
      ValidationUtils.requireValidPort(consensus, "replica.ports.consensus");
      ValidationUtils.requireValidPort(client, "replica.ports.client");
    }
  }

  public record ReplicaKeysSection(@JsonProperty("public") Path publicKeyPath, @JsonProperty("private") Path privateKeyPath, ThresholdKeysSection threshold) {
    public ReplicaKeysSection {
      ValidationUtils.requireNonNull(publicKeyPath, "replica.keys.public");
      ValidationUtils.requireNonBlank(publicKeyPath.toString(), "replica.keys.public");
      ValidationUtils.requireNonNull(privateKeyPath, "replica.keys.private");
      ValidationUtils.requireNonBlank(privateKeyPath.toString(), "replica.keys.private");
      ValidationUtils.requireNonNull(threshold, "replica.keys.threshold");
    }

    public static ReplicaKeysSection forReplica(KeysSection keys, String replicaId) {
      ValidationUtils.requireNonNull(keys, "keys");
      replicaId = ValidationUtils.requireNonBlank(replicaId, "replica.id");

      Path replicaRoot = keys.rootPath().resolve(replicaId);
      return new ReplicaKeysSection(replicaRoot.resolve("public").resolve("replica.pem"), replicaRoot.resolve("private").resolve("replica.pem"),
          new ThresholdKeysSection(replicaRoot.resolve("public").resolve("threshold.pub"), replicaRoot.resolve("private").resolve("threshold.share")));
    }
  }

  public record ThresholdKeysSection(@JsonProperty("public") Path publicKeyPath, @JsonProperty("privateShare") Path privateSharePath) {
    public ThresholdKeysSection {
      ValidationUtils.requireNonNull(publicKeyPath, "replica.keys.threshold.public");
      ValidationUtils.requireNonBlank(publicKeyPath.toString(), "replica.keys.threshold.public");
      ValidationUtils.requireNonNull(privateSharePath, "replica.keys.threshold.privateShare");
      ValidationUtils.requireNonBlank(privateSharePath.toString(), "replica.keys.threshold.privateShare");
    }
  }

  public record ClientSection(String id, long senderId, String host, ClientKeysSection keys, int requestTimeoutMs, List<String> knownReplicas) {
    public ClientSection {
      id = ValidationUtils.requireNonBlank(id, "client.id");
      ValidationUtils.requireNonNegativeLong(senderId, "client.senderId");
      host = ValidationUtils.requireNonBlank(host, "client.host");
      ValidationUtils.requireNonNull(keys, "client.keys");
      ValidationUtils.requireNonNegativeInt(requestTimeoutMs, "client.requestTimeoutMs");
      knownReplicas = List.copyOf(ValidationUtils.requireNonEmpty(knownReplicas, "client.knownReplicas").stream()
          .map(replicaId -> ValidationUtils.requireNonBlank(replicaId, "client.knownReplicas entry")).toList());
    }

    public Path publicKeyPath() {
      return keys.publicKeyPath();
    }

    public Path privateKeyPath() {
      return keys.privateKeyPath();
    }
  }

  public record ClientKeysSection(@JsonProperty("public") Path publicKeyPath, @JsonProperty("private") Path privateKeyPath) {
    public ClientKeysSection {
      ValidationUtils.requireNonNull(publicKeyPath, "client.keys.public");
      ValidationUtils.requireNonBlank(publicKeyPath.toString(), "client.keys.public");
      ValidationUtils.requireNonNull(privateKeyPath, "client.keys.private");
      ValidationUtils.requireNonBlank(privateKeyPath.toString(), "client.keys.private");
    }

    public static ClientKeysSection forClient(KeysSection keys, String clientId) {
      ValidationUtils.requireNonNull(keys, "keys");
      clientId = ValidationUtils.requireNonBlank(clientId, "client.id");

      Path clientRoot = keys.rootPath().resolve(clientId);
      return new ClientKeysSection(clientRoot.resolve("public").resolve("client.pem"), clientRoot.resolve("private").resolve("client.pem"));
    }
  }

  public record TimeoutsSection(int viewChangeMs, int clientCommandWaitMs, int thresholdRoundMs, int fetchNodeMs) {
    public TimeoutsSection {
      ValidationUtils.requirePositiveInt(viewChangeMs, "timeouts.viewChangeMs");
      ValidationUtils.requirePositiveInt(clientCommandWaitMs, "timeouts.clientCommandWaitMs");
      ValidationUtils.requirePositiveInt(thresholdRoundMs, "timeouts.thresholdRoundMs");
      ValidationUtils.requirePositiveInt(fetchNodeMs, "timeouts.fetchNodeMs");
    }
  }

  public record KeysSection(@JsonProperty("root") Path rootPath) {
    public KeysSection {
      ValidationUtils.requireNonNull(rootPath, "keys.root");
      ValidationUtils.requireNonBlank(rootPath.toString(), "keys.root");
    }
  }

  public record StorageSection(@JsonProperty("blocksRoot") Path blocksRootPath) {
    public StorageSection {
      ValidationUtils.requireNonNull(blocksRootPath, "storage.blocksRoot");
      ValidationUtils.requireNonBlank(blocksRootPath.toString(), "storage.blocksRoot");
    }
  }

  public ReplicaSection requireReplicaById(String replicaId) {
    ValidationUtils.requireNonBlank(replicaId, "replicaId");

    return replicas.stream().filter(replica -> replica.id().equals(replicaId)).findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Replica '%s' not found".formatted(replicaId)));
  }

  public ReplicaSection requireReplicaBySenderId(long senderId) {
    ValidationUtils.requireNonNegativeLong(senderId, "senderId");

    return replicas.stream().filter(replica -> replica.senderId() == senderId).findFirst().orElseThrow(() -> new IllegalArgumentException("Unknown replica senderId: " + senderId));
  }

  public int requireReplicaIndexForSenderId(long senderId) {
    ValidationUtils.requireNonNegativeLong(senderId, "senderId");

    for (int i = 0; i < replicas.size(); i++) {
      if (replicas.get(i).senderId() == senderId) {
        return i;
      }
    }

    throw new IllegalArgumentException("Unknown replica senderId: " + senderId);
  }

  public Path blocksDirectoryForReplica(String replicaId) {
    return blocksDirectoryForReplica(requireReplicaById(replicaId));
  }

  public Path blocksDirectoryForReplica(ReplicaSection replica) {
    ValidationUtils.requireNonNull(replica, "replica");
    return storage.blocksRootPath().resolve(replica.id()).resolve("blocks");
  }

  public static ConfigParser load(Path path) throws IOException {
    ValidationUtils.requireNonNull(path, "path");

    try (var input = Files.newInputStream(path)) {
      RawConfig raw = YAML.readValue(input, RawConfig.class);
      return new ConfigParser(raw.system(), normalizeReplicas(raw.replicas(), raw.keys()), normalizeClient(raw.client(), raw.keys()), raw.timeouts(), raw.storage(), raw.keys());
    } catch (IOException exception) {
      throw new IOException("Failed to load config from " + path, exception);
    }
  }

  private static List<ReplicaSection> normalizeReplicas(LinkedHashMap<String, ReplicaInput> replicas, KeysSection keys) {
    ValidationUtils.requireNonNull(replicas, "replicas");
    ValidationUtils.requireNonEmpty(replicas.entrySet(), "replicas");
    ValidationUtils.requireNonNull(keys, "keys");

    return replicas.entrySet().stream().map(entry -> entry.getValue().toReplica(entry.getKey(), keys)).toList();
  }

  private static ClientSection normalizeClient(ClientInput client, KeysSection keys) {
    ValidationUtils.requireNonNull(client, "client");
    ValidationUtils.requireNonNull(keys, "keys");
    return client.toClient(keys);
  }

  private static void validateConsistency(SystemSection system, List<ReplicaSection> replicas, ClientSection client) {
    ValidationUtils.requireExactInt(system.n(), replicas.size(), "system.n");
    ValidationUtils.requireAtLeastInt(system.n(), (3 * system.f()) + 1, "system.n", "3f + 1");

    Set<String> ids = new HashSet<>();
    Set<String> endpoints = new HashSet<>();
    Set<Long> senderIds = new HashSet<>();

    for (ReplicaSection replica : replicas) {
      requireUnique(ids, replica.id(), "Duplicate replica id: " + replica.id());
      requireUnique(senderIds, replica.senderId(), "Duplicate senderId: " + replica.senderId());
      requireUnique(endpoints, endpointKey(replica.host(), replica.consensusPort()), "Duplicate endpoint in replica: " + replica.id());
      requireUnique(endpoints, endpointKey(replica.host(), replica.clientPort()), "Duplicate endpoint in replica: " + replica.id());
    }

    if (senderIds.contains(client.senderId())) {
      throw new IllegalArgumentException("client.senderId conflicts with replica");
    }

    Set<String> seenKnownReplicas = new HashSet<>();
    for (String replicaId : client.knownReplicas()) {
      if (!ids.contains(replicaId)) {
        throw new IllegalArgumentException("client.knownReplicas contains unknown replica '%s'".formatted(replicaId));
      }
      requireUnique(seenKnownReplicas, replicaId, "Duplicate id '%s' in client.knownReplicas".formatted(replicaId));
    }
  }

  private static String endpointKey(String host, int port) {
    return host + ":" + port;
  }

  private static <T> void requireUnique(Set<T> seen, T value, String message) {
    ValidationUtils.requireNonNull(seen, "seen");
    if (!seen.add(value)) {
      throw new IllegalArgumentException(message);
    }
  }

  private record RawConfig(SystemSection system, LinkedHashMap<String, ReplicaInput> replicas, ClientInput client, TimeoutsSection timeouts, StorageSection storage,
      KeysSection keys) {
  }

  private record ReplicaInput(long senderId, String host, ReplicaPortsSection ports, ReplicaKeysSection keys) {
    ReplicaSection toReplica(String replicaId, KeysSection configKeys) {
      ReplicaKeysSection resolvedKeys = keys != null ? keys : ReplicaKeysSection.forReplica(configKeys, replicaId);
      return new ReplicaSection(replicaId, senderId, host, ports, resolvedKeys);
    }
  }

  private record ClientInput(String id, long senderId, String host, ClientKeysSection keys, int requestTimeoutMs, List<String> knownReplicas) {
    ClientSection toClient(KeysSection configKeys) {
      ClientKeysSection resolvedKeys = keys != null ? keys : ClientKeysSection.forClient(configKeys, id);
      return new ClientSection(id, senderId, host, resolvedKeys, requestTimeoutMs, knownReplicas);
    }
  }
}
