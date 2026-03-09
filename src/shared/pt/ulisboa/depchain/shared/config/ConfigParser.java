package pt.ulisboa.depchain.shared.config;

import pt.ulisboa.depchain.shared.utils.ValidationUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public record ConfigParser(SystemSection system, List<ReplicaSection> replicas, ClientSection client, TimeoutsSection timeouts) {
  public record SystemSection(int n, int f) {
  }

  public record ReplicaSection(String id, long senderId, String host, int consensusPort, int clientPort, String publicKeyPath, String privateKeyPath, String thresholdPublicKeyPath,
      String thresholdPrivateSharePath) {
  }

  public record ClientSection(String id, long senderId, String host, String publicKeyPath, String privateKeyPath, int requestTimeoutMs, List<String> knownReplicas) {
  }

  public record TimeoutsSection(int viewChangeMs, int retransmitMs, int maxBackoffMs) {
  }

  public ReplicaSection requireReplica(String replicaId) {
    return replicas.stream().filter(r -> r.id().equals(replicaId)).findFirst().orElseThrow(() -> new IllegalArgumentException("Replica '%s' not found".formatted(replicaId)));
  }

  public ReplicaSection firstKnownReplicaForClient() {
    if (client.knownReplicas().isEmpty()) {
      throw new IllegalArgumentException("No knownReplicas configured in client section");
    }

    return requireReplica(client.knownReplicas().getFirst());
  }

  public static ConfigParser load(Path path) throws IOException {
    ValidationUtils.requireNonNull(path, "path");

    Properties props = new Properties();
    try (var reader = Files.newBufferedReader(path)) {
      props.load(reader);
    }

    var reader = new PropertyReader(props, path);
    var system = readSystem(reader);
    var replicas = readReplicas(reader);
    var client = readClient(reader);
    var timeouts = readTimeouts(reader);

    validateConsistency(system, replicas, client, timeouts, path);
    return new ConfigParser(system, replicas, client, timeouts);
  }

  private static SystemSection readSystem(PropertyReader reader) {
    return new SystemSection(ValidationUtils.requirePositiveInt(Integer.parseInt(reader.str("system.n")), "system.n"),
        ValidationUtils.requireNonNegativeInt(Integer.parseInt(reader.str("system.f")), "system.f"));
  }

  private static List<ReplicaSection> readReplicas(PropertyReader reader) {
    return reader.list("replicas").stream().map(id -> readReplica(reader, id)).toList();
  }

  private static ReplicaSection readReplica(PropertyReader reader, String replicaId) {
    String prefix = "replica." + replicaId + ".";

    return new ReplicaSection(replicaId, ValidationUtils.requireNonNegativeLong(Long.parseLong(reader.str(prefix + "senderId")), prefix + "senderId"), reader.str(prefix + "host"),
        ValidationUtils.requireValidPort(Integer.parseInt(reader.str(prefix + "consensusPort")), prefix + "consensusPort"),
        ValidationUtils.requireValidPort(Integer.parseInt(reader.str(prefix + "clientPort")), prefix + "clientPort"), reader.str(prefix + "publicKeyPath"),
        reader.str(prefix + "privateKeyPath"), reader.str(prefix + "thresholdPublicKeyPath"), reader.str(prefix + "thresholdPrivateSharePath"));
  }

  private static ClientSection readClient(PropertyReader reader) {
    return new ClientSection(reader.str("client.id"), ValidationUtils.requireNonNegativeLong(Long.parseLong(reader.str("client.senderId")), "client.senderId"),
        reader.str("client.host"), reader.str("client.publicKeyPath"), reader.str("client.privateKeyPath"),
        ValidationUtils.requirePositiveInt(Integer.parseInt(reader.str("client.requestTimeoutMs")), "client.requestTimeoutMs"), reader.list("client.knownReplicas"));
  }

  private static TimeoutsSection readTimeouts(PropertyReader reader) {
    return new TimeoutsSection(ValidationUtils.requirePositiveInt(Integer.parseInt(reader.str("timeouts.viewChangeMs")), "timeouts.viewChangeMs"),
        ValidationUtils.requirePositiveInt(Integer.parseInt(reader.str("timeouts.retransmitMs")), "timeouts.retransmitMs"),
        ValidationUtils.requirePositiveInt(Integer.parseInt(reader.str("timeouts.maxBackoffMs")), "timeouts.maxBackoffMs"));
  }

  private record PropertyReader(Properties props, Path path) {
    String str(String key) {
      String rawValue = props.getProperty(key);
      if (rawValue == null) {
        throw new IllegalArgumentException("Missing property '%s' in %s".formatted(key, path));
      }

      String trimmedValue = rawValue.trim();
      return ValidationUtils.requireNonBlank(trimmedValue, propertyLabel(key));
    }

    List<String> list(String key) {
      String rawList = str(key);
      List<String> values = Arrays.stream(rawList.split(",")).map(String::trim).filter(value -> !value.isBlank()).toList();
      return ValidationUtils.requireNonEmpty(values, propertyLabel(key));
    }

    private String propertyLabel(String key) {
      return "Property '%s' in %s".formatted(key, path);
    }
  }

  private static void validateConsistency(SystemSection system, List<ReplicaSection> replicas, ClientSection client, TimeoutsSection timeouts, Path path) {
    ValidationUtils.requireExactInt(system.n(), replicas.size(), "system.n");
    ValidationUtils.requireAtLeastInt(system.n(), (3 * system.f()) + 1, "system.n", "3f + 1");

    Set<String> ids = new HashSet<>(), endpoints = new HashSet<>();
    Set<Long> senderIds = new HashSet<>();

    for (ReplicaSection r : replicas) {
      if (!ids.add(r.id())) {
        throw new IllegalArgumentException("Duplicate replica id: " + r.id());
      }
      if (!senderIds.add(r.senderId())) {
        throw new IllegalArgumentException("Duplicate senderId: " + r.senderId());
      }
      if (!endpoints.add(r.host() + ":" + r.consensusPort()) || !endpoints.add(r.host() + ":" + r.clientPort())) {
        throw new IllegalArgumentException("Duplicate endpoint in replica: " + r.id());
      }
    }

    if (senderIds.contains(client.senderId())) {
      throw new IllegalArgumentException("client.senderId conflicts with replica");
    }
    ValidationUtils.requireNonEmpty(client.knownReplicas(), "client.knownReplicas");

    Set<String> seenKnownReplicas = new HashSet<>();
    for (String replicaId : client.knownReplicas()) {
      if (!ids.contains(replicaId)) {
        throw new IllegalArgumentException("client.knownReplicas contains unknown replica '%s'".formatted(replicaId));
      }
      if (!seenKnownReplicas.add(replicaId)) {
        throw new IllegalArgumentException("Duplicate id '%s' in client.knownReplicas".formatted(replicaId));
      }
    }

    ValidationUtils.requireAtLeast(timeouts.maxBackoffMs(), timeouts.retransmitMs(), "maxBackoffMs", "retransmitMs");
  }
}
