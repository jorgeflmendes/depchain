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

public record ConfigParser(SystemSection system, List<ReplicaSection> replicas, ClientSection client, TimeoutsSection timeouts) {
  private static final ObjectMapper YAML = YAMLMapper.builder().build();

  public ConfigParser {
    ValidationUtils.requireAllNonNull(named("system", system), named("replicas", replicas), named("client", client), named("timeouts", timeouts));
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

    public String publicKeyPath() {
      return keys.publicKeyPath();
    }

    public String privateKeyPath() {
      return keys.privateKeyPath();
    }

    public String thresholdPublicKeyPath() {
      return keys.threshold().publicKeyPath();
    }

    public String thresholdPrivateSharePath() {
      return keys.threshold().privateSharePath();
    }
  }

  public record ReplicaPortsSection(int consensus, int client) {
    public ReplicaPortsSection {
      ValidationUtils.requireValidPort(consensus, "replica.ports.consensus");
      ValidationUtils.requireValidPort(client, "replica.ports.client");
    }
  }

  public record ReplicaKeysSection(@JsonProperty("public") String publicKeyPath, @JsonProperty("private") String privateKeyPath, ThresholdKeysSection threshold) {
    public ReplicaKeysSection {
      publicKeyPath = ValidationUtils.requireNonBlank(publicKeyPath, "replica.keys.public");
      privateKeyPath = ValidationUtils.requireNonBlank(privateKeyPath, "replica.keys.private");
      ValidationUtils.requireNonNull(threshold, "replica.keys.threshold");
    }
  }

  public record ThresholdKeysSection(@JsonProperty("public") String publicKeyPath, String privateShare) {
    public ThresholdKeysSection {
      publicKeyPath = ValidationUtils.requireNonBlank(publicKeyPath, "replica.keys.threshold.public");
      privateShare = ValidationUtils.requireNonBlank(privateShare, "replica.keys.threshold.privateShare");
    }

    public String privateSharePath() {
      return privateShare;
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

    public String publicKeyPath() {
      return keys.publicKeyPath();
    }

    public String privateKeyPath() {
      return keys.privateKeyPath();
    }
  }

  public record ClientKeysSection(@JsonProperty("public") String publicKeyPath, @JsonProperty("private") String privateKeyPath) {
    public ClientKeysSection {
      publicKeyPath = ValidationUtils.requireNonBlank(publicKeyPath, "client.keys.public");
      privateKeyPath = ValidationUtils.requireNonBlank(privateKeyPath, "client.keys.private");
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

  public ReplicaSection requireReplica(String replicaId) {
    return requireReplicaById(replicaId);
  }

  public ReplicaSection requireReplicaById(String replicaId) {
    ValidationUtils.requireNonBlank(replicaId, "replicaId");

    return ValidationUtils.requirePresent(replicas.stream().filter(replica -> replica.id().equals(replicaId)).findFirst().orElse(null),
        "Replica '%s' not found".formatted(replicaId));
  }

  public ReplicaSection requireReplicaBySenderId(int senderId) {
    return requireReplicaBySenderId((long) senderId);
  }

  public ReplicaSection requireReplicaBySenderId(long senderId) {
    ValidationUtils.requireNonNegativeLong(senderId, "senderId");

    return ValidationUtils.requirePresent(replicas.stream().filter(replica -> replica.senderId() == senderId).findFirst().orElse(null),
        "Unknown replica senderId: " + senderId);
  }

  public int replicaIndexForSenderId(int senderId) {
    return requireReplicaIndexForSenderId(senderId);
  }

  public int replicaIndexForSenderId(long senderId) {
    return requireReplicaIndexForSenderId(senderId);
  }

  public int requireReplicaIndexForSenderId(int senderId) {
    return requireReplicaIndexForSenderId((long) senderId);
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

  public ReplicaSection firstKnownReplicaForClient() {
    return requireFirstKnownReplicaForClient();
  }

  public ReplicaSection requireFirstKnownReplicaForClient() {
    return requireReplicaById(client.knownReplicas().getFirst());
  }

  public static ConfigParser load(Path path) throws IOException {
    ValidationUtils.requireNonNull(path, "path");

    try (var input = Files.newInputStream(path)) {
      RawConfig raw = YAML.readValue(input, RawConfig.class);
      return new ConfigParser(raw.system(), normalizeReplicas(raw.replicas()), raw.client(), raw.timeouts());
    } catch (IOException exception) {
      throw new IOException("Failed to load config from " + path, exception);
    }
  }

  private static List<ReplicaSection> normalizeReplicas(LinkedHashMap<String, ReplicaInput> replicas) {
    ValidationUtils.requireNonNull(replicas, "replicas");
    ValidationUtils.requireNonEmpty(replicas.entrySet(), "replicas");

    return replicas.entrySet().stream().map(entry -> entry.getValue().toReplica(entry.getKey())).toList();
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
      ValidationUtils.requirePresent(ids.contains(replicaId) ? replicaId : null, "client.knownReplicas contains unknown replica '%s'".formatted(replicaId));
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

  private record RawConfig(SystemSection system, LinkedHashMap<String, ReplicaInput> replicas, ClientSection client, TimeoutsSection timeouts) {
  }

  private record ReplicaInput(long senderId, String host, ReplicaPortsSection ports, ReplicaKeysSection keys) {
    ReplicaSection toReplica(String replicaId) {
      return new ReplicaSection(replicaId, senderId, host, ports, keys);
    }
  }
}
