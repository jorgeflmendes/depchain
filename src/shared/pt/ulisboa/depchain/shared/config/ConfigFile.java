package pt.ulisboa.depchain.shared.config;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class ConfigFile {
  private final SystemSection system;
  private final List<ReplicaSection> replicas;
  private final ClientSection client;
  private final TimeoutsSection timeouts;
  private final StubbornSection stubborn;
  private final NetworkSection network;

  private ConfigFile(
      SystemSection system,
      List<ReplicaSection> replicas,
      ClientSection client,
      TimeoutsSection timeouts,
      StubbornSection stubborn,
      NetworkSection network) {
    this.system = system;
    this.replicas = List.copyOf(replicas);
    this.client = client;
    this.timeouts = timeouts;
    this.stubborn = stubborn;
    this.network = network;
  }

  public SystemSection system() {
    return system;
  }

  public List<ReplicaSection> replicas() {
    return replicas;
  }

  public ClientSection client() {
    return client;
  }

  public TimeoutsSection timeouts() {
    return timeouts;
  }

  public StubbornSection stubborn() {
    return stubborn;
  }

  public NetworkSection network() {
    return network;
  }

  public ReplicaSection requireReplica(String replicaId) {
    return replicas.stream()
        .filter(r -> r.id().equals(replicaId))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Replica '%s' not found in config".formatted(replicaId)));
  }

  public ReplicaSection firstKnownReplicaForClient() {
    if (client.knownReplicas().isEmpty()) {
      throw new IllegalArgumentException("No knownReplicas configured in client section");
    }
    return requireReplica(client.knownReplicas().getFirst());
  }

  public static ConfigFile load(Path path) throws IOException {
    Objects.requireNonNull(path, "path must not be null");
    List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
    return new Parser(path, lines).parse();
  }

  private static final class Parser {
    private final Path path;
    private final List<String> lines;
    private int index = 0;

    private Parser(Path path, List<String> lines) {
      this.path = path;
      this.lines = lines;
    }

    private ConfigFile parse() {
      MutableSystemSection system = null;
      List<ReplicaSection> replicas = null;
      MutableClientSection client = null;
      MutableTimeoutsSection timeouts = null;
      MutableStubbornSection stubborn = null;
      MutableNetworkSection network = null;
      Set<String> seenTopLevelSections = new HashSet<>();

      while (hasMoreLines()) {
        String line = normalizeLine(currentRawLine(), currentLineNumber());
        if (line.isBlank()) {
          index++;
          continue;
        }

        int indent = indentationOf(line, currentLineNumber());
        if (indent != 0) {
          fail("Top-level section must start at column 1");
        }

        if (!line.trim().endsWith(":")) {
          fail("Expected top-level section ending with ':'");
        }

        String sectionName = line.trim().substring(0, line.trim().length() - 1);
        if (!seenTopLevelSections.add(sectionName)) {
          fail("Duplicate top-level section '%s'".formatted(sectionName));
        }

        index++;
        switch (sectionName) {
          case "system" -> system = parseSystemSection();
          case "replicas" -> replicas = parseReplicasSection();
          case "client" -> client = parseClientSection();
          case "timeouts" -> timeouts = parseTimeoutsSection();
          case "stubborn" -> stubborn = parseStubbornSection();
          case "network" -> network = parseNetworkSection();
          default -> fail("Unknown top-level section '%s'".formatted(sectionName));
        }
      }

      if (system == null) {
        throw new IllegalArgumentException("Missing required section 'system' in " + path);
      }
      if (replicas == null) {
        throw new IllegalArgumentException("Missing required section 'replicas' in " + path);
      }
      if (client == null) {
        throw new IllegalArgumentException("Missing required section 'client' in " + path);
      }
      if (timeouts == null) {
        throw new IllegalArgumentException("Missing required section 'timeouts' in " + path);
      }
      if (stubborn == null) {
        throw new IllegalArgumentException("Missing required section 'stubborn' in " + path);
      }
      if (network == null) {
        throw new IllegalArgumentException("Missing required section 'network' in " + path);
      }

      SystemSection parsedSystem = system.toRecord(path);
      ClientSection parsedClient = client.toRecord(path);
      TimeoutsSection parsedTimeouts = timeouts.toRecord(path);
      StubbornSection parsedStubborn = stubborn.toRecord(path);
      NetworkSection parsedNetwork = network.toRecord(path);

      validateConsistency(parsedSystem, replicas, parsedClient, parsedTimeouts, parsedStubborn, parsedNetwork, path);
      return new ConfigFile(parsedSystem, replicas, parsedClient, parsedTimeouts, parsedStubborn, parsedNetwork);
    }

    private MutableSystemSection parseSystemSection() {
      MutableSystemSection result = new MutableSystemSection();
      while (hasMoreLines()) {
        String line = normalizeLine(currentRawLine(), currentLineNumber());
        if (line.isBlank()) {
          index++;
          continue;
        }

        int indent = indentationOf(line, currentLineNumber());
        if (indent == 0) {
          break;
        }
        if (indent != 2) {
          fail("Invalid indentation in 'system' section (expected 2 spaces)");
        }

        KeyValue kv = parseKeyValue(line.trim());
        switch (kv.key()) {
          case "name" -> result.name = requireNonBlank(kv.value(), "system.name");
          case "environment" -> result.environment = requireNonBlank(kv.value(), "system.environment");
          case "n" -> result.n = parsePositiveInt(kv.value(), "system.n");
          case "f" -> result.f = parseNonNegativeInt(kv.value(), "system.f");
          case "leaderElection" ->
              result.leaderElection = requireNonBlank(kv.value(), "system.leaderElection");
          case "baseView" -> result.baseView = parsePositiveInt(kv.value(), "system.baseView");
          default -> fail("Unknown key in 'system' section: '%s'".formatted(kv.key()));
        }
        index++;
      }
      return result;
    }

    private List<ReplicaSection> parseReplicasSection() {
      List<ReplicaSection> result = new ArrayList<>();

      while (hasMoreLines()) {
        String line = normalizeLine(currentRawLine(), currentLineNumber());
        if (line.isBlank()) {
          index++;
          continue;
        }

        int indent = indentationOf(line, currentLineNumber());
        if (indent == 0) {
          break;
        }
        if (indent != 2) {
          fail("Invalid indentation in 'replicas' section (expected 2 spaces for list items)");
        }

        String trimmed = line.trim();
        if (!trimmed.startsWith("-")) {
          fail("Expected list item in 'replicas' section");
        }

        MutableReplicaSection replica = new MutableReplicaSection();
        String inlineContent = trimmed.substring(1).trim();
        if (!inlineContent.isBlank()) {
          KeyValue firstKeyValue = parseKeyValue(inlineContent);
          assignReplicaKey(replica, firstKeyValue);
        }
        index++;

        while (hasMoreLines()) {
          String childLine = normalizeLine(currentRawLine(), currentLineNumber());
          if (childLine.isBlank()) {
            index++;
            continue;
          }

          int childIndent = indentationOf(childLine, currentLineNumber());
          if (childIndent <= 2) {
            break;
          }
          if (childIndent != 4) {
            fail("Invalid indentation for replica fields (expected 4 spaces)");
          }

          KeyValue kv = parseKeyValue(childLine.trim());
          assignReplicaKey(replica, kv);
          index++;
        }

        result.add(replica.toRecord(path));
      }

      if (result.isEmpty()) {
        throw new IllegalArgumentException("Section 'replicas' must contain at least one replica in " + path);
      }
      return List.copyOf(result);
    }

    private MutableClientSection parseClientSection() {
      MutableClientSection result = new MutableClientSection();

      while (hasMoreLines()) {
        String line = normalizeLine(currentRawLine(), currentLineNumber());
        if (line.isBlank()) {
          index++;
          continue;
        }

        int indent = indentationOf(line, currentLineNumber());
        if (indent == 0) {
          break;
        }
        if (indent != 2) {
          fail("Invalid indentation in 'client' section (expected 2 spaces)");
        }

        KeyValue kv = parseKeyValue(line.trim());
        if (kv.key().equals("knownReplicas")) {
          if (!kv.value().isBlank()) {
            fail("'client.knownReplicas' must be declared as a YAML list");
          }
          index++;
          while (hasMoreLines()) {
            String itemLine = normalizeLine(currentRawLine(), currentLineNumber());
            if (itemLine.isBlank()) {
              index++;
              continue;
            }
            int itemIndent = indentationOf(itemLine, currentLineNumber());
            if (itemIndent <= 2) {
              break;
            }
            if (itemIndent != 4) {
              fail("Invalid indentation for knownReplicas items (expected 4 spaces)");
            }

            String itemTrimmed = itemLine.trim();
            if (!itemTrimmed.startsWith("- ")) {
              fail("Expected list item under 'client.knownReplicas'");
            }

            String replicaId = unquote(itemTrimmed.substring(2).trim());
            if (replicaId.isBlank()) {
              fail("knownReplicas item cannot be blank");
            }
            result.knownReplicas.add(replicaId);
            index++;
          }
          continue;
        }

        switch (kv.key()) {
          case "id" -> result.id = requireNonBlank(kv.value(), "client.id");
          case "host" -> result.host = requireNonBlank(kv.value(), "client.host");
          case "requestTimeoutMs" ->
              result.requestTimeoutMs = parsePositiveInt(kv.value(), "client.requestTimeoutMs");
          default -> fail("Unknown key in 'client' section: '%s'".formatted(kv.key()));
        }
        index++;
      }

      return result;
    }

    private MutableTimeoutsSection parseTimeoutsSection() {
      MutableTimeoutsSection result = new MutableTimeoutsSection();

      while (hasMoreLines()) {
        String line = normalizeLine(currentRawLine(), currentLineNumber());
        if (line.isBlank()) {
          index++;
          continue;
        }

        int indent = indentationOf(line, currentLineNumber());
        if (indent == 0) {
          break;
        }
        if (indent != 2) {
          fail("Invalid indentation in 'timeouts' section (expected 2 spaces)");
        }

        KeyValue kv = parseKeyValue(line.trim());
        switch (kv.key()) {
          case "viewChangeMs" -> result.viewChangeMs = parsePositiveInt(kv.value(), "timeouts.viewChangeMs");
          case "retransmitMs" -> result.retransmitMs = parsePositiveInt(kv.value(), "timeouts.retransmitMs");
          case "maxBackoffMs" -> result.maxBackoffMs = parsePositiveInt(kv.value(), "timeouts.maxBackoffMs");
          default -> fail("Unknown key in 'timeouts' section: '%s'".formatted(kv.key()));
        }
        index++;
      }

      return result;
    }

    private MutableNetworkSection parseNetworkSection() {
      MutableNetworkSection result = new MutableNetworkSection();

      while (hasMoreLines()) {
        String line = normalizeLine(currentRawLine(), currentLineNumber());
        if (line.isBlank()) {
          index++;
          continue;
        }

        int indent = indentationOf(line, currentLineNumber());
        if (indent == 0) {
          break;
        }
        if (indent != 2) {
          fail("Invalid indentation in 'network' section (expected 2 spaces)");
        }

        KeyValue kv = parseKeyValue(line.trim());
        switch (kv.key()) {
          case "maxPacketSize" -> result.maxPacketSize = parsePositiveInt(kv.value(), "network.maxPacketSize");
          default -> fail("Unknown key in 'network' section: '%s'".formatted(kv.key()));
        }
        index++;
      }

      return result;
    }

    private MutableStubbornSection parseStubbornSection() {
      MutableStubbornSection result = new MutableStubbornSection();

      while (hasMoreLines()) {
        String line = normalizeLine(currentRawLine(), currentLineNumber());
        if (line.isBlank()) {
          index++;
          continue;
        }

        int indent = indentationOf(line, currentLineNumber());
        if (indent == 0) {
          break;
        }
        if (indent != 2) {
          fail("Invalid indentation in 'stubborn' section (expected 2 spaces)");
        }

        KeyValue kv = parseKeyValue(line.trim());
        switch (kv.key()) {
          case "baseDelayMs" -> result.baseDelayMs = parsePositiveLong(kv.value(), "stubborn.baseDelayMs");
          case "maxDelayMs" -> result.maxDelayMs = parsePositiveLong(kv.value(), "stubborn.maxDelayMs");
          case "jitterRatio" -> result.jitterRatio = parseNonNegativeDouble(kv.value(), "stubborn.jitterRatio");
          case "maxPending" -> result.maxPending = parsePositiveInt(kv.value(), "stubborn.maxPending");
          case "heapCompactMinSize" ->
              result.heapCompactMinSize = parsePositiveInt(kv.value(), "stubborn.heapCompactMinSize");
          default -> fail("Unknown key in 'stubborn' section: '%s'".formatted(kv.key()));
        }
        index++;
      }

      return result;
    }

    private void assignReplicaKey(MutableReplicaSection replica, KeyValue kv) {
      switch (kv.key()) {
        case "id" -> replica.id = requireNonBlank(kv.value(), "replicas.id");
        case "host" -> replica.host = requireNonBlank(kv.value(), "replicas.host");
        case "consensusPort" -> replica.consensusPort = parsePort(kv.value(), "replicas.consensusPort");
        case "clientPort" -> replica.clientPort = parsePort(kv.value(), "replicas.clientPort");
        case "publicKeyPath" -> replica.publicKeyPath = requireNonBlank(kv.value(), "replicas.publicKeyPath");
        default -> fail("Unknown key in 'replicas' item: '%s'".formatted(kv.key()));
      }
    }

    private String currentRawLine() {
      return lines.get(index);
    }

    private int currentLineNumber() {
      return index + 1;
    }

    private boolean hasMoreLines() {
      return index < lines.size();
    }

    private void fail(String message) {
      throw new IllegalArgumentException(
          "Invalid config at %s:%d: %s".formatted(path, currentLineNumber(), message));
    }
  }

  private static void validateConsistency(
      SystemSection system,
      List<ReplicaSection> replicas,
      ClientSection client,
      TimeoutsSection timeouts,
      StubbornSection stubborn,
      NetworkSection network,
      Path path) {
    if (system.n() != replicas.size()) {
      throw new IllegalArgumentException(
          "system.n (%d) must match number of replicas (%d) in %s"
              .formatted(system.n(), replicas.size(), path));
    }

    if (system.n() < (3 * system.f()) + 1) {
      throw new IllegalArgumentException(
          "Invalid Byzantine threshold in %s: require n >= 3f + 1 (got n=%d, f=%d)"
              .formatted(path, system.n(), system.f()));
    }

    Set<String> replicaIds = new HashSet<>();
    Set<String> endpoints = new HashSet<>();
    for (ReplicaSection replica : replicas) {
      if (!replicaIds.add(replica.id())) {
        throw new IllegalArgumentException(
            "Duplicate replica id '%s' in %s".formatted(replica.id(), path));
      }

      String consensusEndpoint = replica.host() + ":" + replica.consensusPort();
      if (!endpoints.add(consensusEndpoint)) {
        throw new IllegalArgumentException(
            "Duplicate replica consensus endpoint '%s' in %s".formatted(consensusEndpoint, path));
      }

      String clientEndpoint = replica.host() + ":" + replica.clientPort();
      if (!endpoints.add(clientEndpoint)) {
        throw new IllegalArgumentException(
            "Duplicate replica client endpoint '%s' in %s".formatted(clientEndpoint, path));
      }
    }

    if (client.knownReplicas().isEmpty()) {
      throw new IllegalArgumentException("client.knownReplicas must not be empty in " + path);
    }

    Set<String> uniqueKnownReplicas = new HashSet<>();
    for (String replicaId : client.knownReplicas()) {
      if (!replicaIds.contains(replicaId)) {
        throw new IllegalArgumentException(
            "client.knownReplicas contains unknown replica '%s' in %s"
                .formatted(replicaId, path));
      }
      if (!uniqueKnownReplicas.add(replicaId)) {
        throw new IllegalArgumentException(
            "Duplicate id '%s' in client.knownReplicas in %s".formatted(replicaId, path));
      }
    }

    if (timeouts.maxBackoffMs() < timeouts.retransmitMs()) {
      throw new IllegalArgumentException(
          "timeouts.maxBackoffMs must be >= timeouts.retransmitMs in " + path);
    }

    if (stubborn.maxDelayMs() < stubborn.baseDelayMs()) {
      throw new IllegalArgumentException(
          "stubborn.maxDelayMs must be >= stubborn.baseDelayMs in " + path);
    }

    if (stubborn.jitterRatio() >= 1.0d) {
      throw new IllegalArgumentException(
          "stubborn.jitterRatio must be < 1.0 in " + path);
    }

    if (network.maxPacketSize() > 65507) {
      throw new IllegalArgumentException(
          "network.maxPacketSize must be <= 65507 for UDP payloads in " + path);
    }
  }

  private static String normalizeLine(String rawLine, int lineNumber) {
    if (rawLine.indexOf('\t') >= 0) {
      throw new IllegalArgumentException("Invalid config at line " + lineNumber + ": tabs are not allowed");
    }
    return stripInlineComment(rawLine).stripTrailing();
  }

  private static int indentationOf(String line, int lineNumber) {
    int spaces = 0;
    while (spaces < line.length() && line.charAt(spaces) == ' ') {
      spaces++;
    }
    if (spaces % 2 != 0) {
      throw new IllegalArgumentException(
          "Invalid config at line " + lineNumber + ": indentation must use multiples of 2 spaces");
    }
    return spaces;
  }

  private static KeyValue parseKeyValue(String trimmedLine) {
    int separator = trimmedLine.indexOf(':');
    if (separator <= 0) {
      throw new IllegalArgumentException("Expected key/value entry, got: " + trimmedLine);
    }

    String key = trimmedLine.substring(0, separator).trim();
    String rawValue = trimmedLine.substring(separator + 1).trim();
    return new KeyValue(key, unquote(rawValue));
  }

  private static String stripInlineComment(String line) {
    boolean inSingleQuotes = false;
    boolean inDoubleQuotes = false;

    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);
      if (c == '\'' && !inDoubleQuotes) {
        inSingleQuotes = !inSingleQuotes;
      } else if (c == '"' && !inSingleQuotes) {
        inDoubleQuotes = !inDoubleQuotes;
      } else if (c == '#' && !inSingleQuotes && !inDoubleQuotes) {
        return line.substring(0, i);
      }
    }
    return line;
  }

  private static String unquote(String value) {
    if (value.length() >= 2) {
      if ((value.startsWith("\"") && value.endsWith("\""))
          || (value.startsWith("'") && value.endsWith("'"))) {
        return value.substring(1, value.length() - 1);
      }
    }
    return value;
  }

  private static String requireNonBlank(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("Field '%s' must not be blank".formatted(field));
    }
    return value;
  }

  private static int parseNonNegativeInt(String value, String field) {
    int parsed = parseInteger(value, field);
    return ValidationUtils.requireNonNegativeInt(parsed, "Field '%s'".formatted(field));
  }

  private static int parsePositiveInt(String value, String field) {
    int parsed = parseInteger(value, field);
    return ValidationUtils.requirePositiveInt(parsed, "Field '%s'".formatted(field));
  }

  private static int parsePort(String value, String field) {
    int port = parseInteger(value, field);
    return ValidationUtils.requireValidPort(port, "Field '%s'".formatted(field));
  }

  private static long parsePositiveLong(String value, String field) {
    long parsed = parseLong(value, field);
    if (parsed <= 0L) {
      throw new IllegalArgumentException("Field '%s' must be > 0".formatted(field));
    }
    return parsed;
  }

  private static double parseNonNegativeDouble(String value, String field) {
    double parsed = parseDouble(value, field);
    if (parsed < 0.0d) {
      throw new IllegalArgumentException("Field '%s' must be >= 0".formatted(field));
    }
    return parsed;
  }

  private static int parseInteger(String value, String field) {
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Field '%s' is not a valid integer: %s".formatted(field, value), e);
    }
  }

  private static long parseLong(String value, String field) {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Field '%s' is not a valid long: %s".formatted(field, value), e);
    }
  }

  private static double parseDouble(String value, String field) {
    try {
      return Double.parseDouble(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Field '%s' is not a valid decimal number: %s".formatted(field, value), e);
    }
  }

  private record KeyValue(String key, String value) {}

  private static final class MutableSystemSection {
    private String name;
    private String environment;
    private Integer n;
    private Integer f;
    private String leaderElection;
    private Integer baseView;

    private SystemSection toRecord(Path path) {
      if (name == null
          || environment == null
          || n == null
          || f == null
          || leaderElection == null
          || baseView == null) {
        throw new IllegalArgumentException("Section 'system' is incomplete in " + path);
      }
      return new SystemSection(name, environment, n, f, leaderElection, baseView);
    }
  }

  private static final class MutableReplicaSection {
    private String id;
    private String host;
    private Integer consensusPort;
    private Integer clientPort;
    private String publicKeyPath;

    private ReplicaSection toRecord(Path path) {
      if (id == null
          || host == null
          || consensusPort == null
          || clientPort == null
          || publicKeyPath == null) {
        throw new IllegalArgumentException("Replica entry is incomplete in " + path);
      }
      return new ReplicaSection(id, host, consensusPort, clientPort, publicKeyPath);
    }
  }

  private static final class MutableClientSection {
    private String id;
    private String host;
    private Integer requestTimeoutMs;
    private final List<String> knownReplicas = new ArrayList<>();

    private ClientSection toRecord(Path path) {
      if (id == null || host == null || requestTimeoutMs == null) {
        throw new IllegalArgumentException("Section 'client' is incomplete in " + path);
      }
      return new ClientSection(id, host, requestTimeoutMs, List.copyOf(knownReplicas));
    }
  }

  private static final class MutableTimeoutsSection {
    private Integer viewChangeMs;
    private Integer retransmitMs;
    private Integer maxBackoffMs;

    private TimeoutsSection toRecord(Path path) {
      if (viewChangeMs == null || retransmitMs == null || maxBackoffMs == null) {
        throw new IllegalArgumentException("Section 'timeouts' is incomplete in " + path);
      }
      return new TimeoutsSection(viewChangeMs, retransmitMs, maxBackoffMs);
    }
  }

  private static final class MutableNetworkSection {
    private Integer maxPacketSize;

    private NetworkSection toRecord(Path path) {
      if (maxPacketSize == null) {
        throw new IllegalArgumentException("Section 'network' is incomplete in " + path);
      }
      return new NetworkSection(maxPacketSize);
    }
  }

  private static final class MutableStubbornSection {
    private Long baseDelayMs;
    private Long maxDelayMs;
    private Double jitterRatio;
    private Integer maxPending;
    private Integer heapCompactMinSize;

    private StubbornSection toRecord(Path path) {
      if (baseDelayMs == null
          || maxDelayMs == null
          || jitterRatio == null
          || maxPending == null
          || heapCompactMinSize == null) {
        throw new IllegalArgumentException("Section 'stubborn' is incomplete in " + path);
      }
      return new StubbornSection(baseDelayMs, maxDelayMs, jitterRatio, maxPending, heapCompactMinSize);
    }
  }

  public record SystemSection(
      String name, String environment, int n, int f, String leaderElection, int baseView) {}

  public record ReplicaSection(
      String id, String host, int consensusPort, int clientPort, String publicKeyPath) {}

  public record ClientSection(
      String id, String host, int requestTimeoutMs, List<String> knownReplicas) {}

  public record TimeoutsSection(int viewChangeMs, int retransmitMs, int maxBackoffMs) {}

  public record StubbornSection(
      long baseDelayMs, long maxDelayMs, double jitterRatio, int maxPending, int heapCompactMinSize) {}

  public record NetworkSection(int maxPacketSize) {}
}
