package pt.ulisboa.depchain.shared.config;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class ConfigParser {
  private final SystemSection system;
  private final List<ReplicaSection> replicas;
  private final ClientSection client;
  private final TimeoutsSection timeouts;

  private ConfigParser(SystemSection system, List<ReplicaSection> replicas, ClientSection client, TimeoutsSection timeouts) {
    this.system = system;
    this.replicas = List.copyOf(replicas);
    this.client = client;
    this.timeouts = timeouts;
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

  public ReplicaSection requireReplica(String replicaId) {
    return replicas.stream()
        .filter(r -> r.id().equals(replicaId))
        .findFirst()
        .orElseThrow(
            () -> new IllegalArgumentException("Replica '%s' not found in config".formatted(replicaId)));
  }
  
  public ReplicaSection firstKnownReplicaForClient() {
    if (client.knownReplicas().isEmpty()) {
      throw new IllegalArgumentException("No knownReplicas configured in client section");
    }
    return requireReplica(client.knownReplicas().getFirst());
  }

  // Static loader for YAML config files
  public static ConfigParser load(Path path) throws IOException {
    ValidationUtils.requireNonNull(path, "path");
    List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
    return new Parser(path, lines).parse();
  }

  // YAML parser
  private static final class Parser {
    private final Path path;
    private final List<String> lines;
    private int index = 0;

    private Parser(Path path, List<String> lines) {
      this.path = path;
      this.lines = lines;
    }

    private ConfigParser parse() {
      MutableSystemSection system = null;
      List<ReplicaSection> replicas = null;
      MutableClientSection client = null;
      MutableTimeoutsSection timeouts = null;
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
          default -> fail("Unknown top-level section '%s'".formatted(sectionName));
        }
      }

      system = ValidationUtils.requirePresent(system, "Missing required section 'system' in " + path);
      replicas =
          ValidationUtils.requirePresent(replicas, "Missing required section 'replicas' in " + path);
      client = ValidationUtils.requirePresent(client, "Missing required section 'client' in " + path);
      timeouts =
          ValidationUtils.requirePresent(timeouts, "Missing required section 'timeouts' in " + path);

      SystemSection parsedSystem = system.toRecord(path);
      ClientSection parsedClient = client.toRecord(path);
      TimeoutsSection parsedTimeouts = timeouts.toRecord(path);

      validateConsistency(parsedSystem, replicas, parsedClient, parsedTimeouts, path);
      return new ConfigParser(parsedSystem, replicas, parsedClient, parsedTimeouts);
    }

    // YAML section: system-
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
          case "leaderElection" -> result.leaderElection = requireNonBlank(kv.value(), "system.leaderElection");
          case "baseView" -> result.baseView = parsePositiveInt(kv.value(), "system.baseView");
          default -> fail("Unknown key in 'system' section: '%s'".formatted(kv.key()));
        }
        index++;
      }
      return result;
    }

    // YAML section: replicas
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

    // YAML section: client 
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

    // YAML section: timeouts
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

    // Common parser helpers-
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

  // Cross-section validation
  private static void validateConsistency(SystemSection system, List<ReplicaSection> replicas, ClientSection client, TimeoutsSection timeouts, Path path) {
    if (system.n() != replicas.size()) {
      throw new IllegalArgumentException("system.n (%d) must match number of replicas (%d) in %s".formatted(system.n(), replicas.size(), path));
    }

    if (system.n() < (3 * system.f()) + 1) {
      throw new IllegalArgumentException("Invalid Byzantine threshold in %s: require n >= 3f + 1 (got n=%d, f=%d)".formatted(path, system.n(), system.f()));
    }

    Set<String> replicaIds = new HashSet<>();
    Set<String> endpoints = new HashSet<>();
    for (ReplicaSection replica : replicas) {
      if (!replicaIds.add(replica.id())) {
        throw new IllegalArgumentException("Duplicate replica id '%s' in %s".formatted(replica.id(), path));
      }

      String consensusEndpoint = replica.host() + ":" + replica.consensusPort();
      if (!endpoints.add(consensusEndpoint)) {
        throw new IllegalArgumentException("Duplicate replica consensus endpoint '%s' in %s".formatted(consensusEndpoint, path));
      }

      String clientEndpoint = replica.host() + ":" + replica.clientPort();
      if (!endpoints.add(clientEndpoint)) {
        throw new IllegalArgumentException("Duplicate replica client endpoint '%s' in %s".formatted(clientEndpoint, path));
      }
    }

    if (client.knownReplicas().isEmpty()) {
      throw new IllegalArgumentException("client.knownReplicas must not be empty in " + path);
    }

    Set<String> uniqueKnownReplicas = new HashSet<>();
    for (String replicaId : client.knownReplicas()) {
      if (!replicaIds.contains(replicaId)) {
        throw new IllegalArgumentException("client.knownReplicas contains unknown replica '%s' in %s".formatted(replicaId, path));
      }
      if (!uniqueKnownReplicas.add(replicaId)) {
        throw new IllegalArgumentException("Duplicate id '%s' in client.knownReplicas in %s".formatted(replicaId, path));
      }
    }

    try {
      ValidationUtils.requireAtLeast(timeouts.maxBackoffMs(), timeouts.retransmitMs(), "timeouts.maxBackoffMs", "timeouts.retransmitMs");
    } catch (IllegalArgumentException exception) {
      throw new IllegalArgumentException(exception.getMessage() + " in " + path, exception);
    }

  }

  // Parsing helpers
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
      throw new IllegalArgumentException("Invalid config at line " + lineNumber + ": indentation must use multiples of 2 spaces");
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
      if ((value.startsWith("\"") && value.endsWith("\"")) || (value.startsWith("'") && value.endsWith("'"))) {
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

  private static int parseInteger(String value, String field) {
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Field '%s' is not a valid integer: %s".formatted(field, value), e);
    }
  }

  private record KeyValue(String key, String value) {}

  // Mutable section builders
  private static final class MutableSystemSection {
    private String name;
    private String environment;
    private Integer n;
    private Integer f;
    private String leaderElection;
    private Integer baseView;

    private SystemSection toRecord(Path path) {
      ValidationUtils.requireAllPresent(
          "Section 'system' is incomplete in " + path, name, environment, n, f, leaderElection, baseView);
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
      ValidationUtils.requireAllPresent(
          "Replica entry is incomplete in " + path, id, host, consensusPort, clientPort, publicKeyPath);
      return new ReplicaSection(id, host, consensusPort, clientPort, publicKeyPath);
    }
  }

  private static final class MutableClientSection {
    private String id;
    private String host;
    private Integer requestTimeoutMs;
    private final List<String> knownReplicas = new ArrayList<>();

    private ClientSection toRecord(Path path) {
      ValidationUtils.requireAllPresent(
          "Section 'client' is incomplete in " + path, id, host, requestTimeoutMs);
      return new ClientSection(id, host, requestTimeoutMs, List.copyOf(knownReplicas));
    }
  }

  private static final class MutableTimeoutsSection {
    private Integer viewChangeMs;
    private Integer retransmitMs;
    private Integer maxBackoffMs;

    private TimeoutsSection toRecord(Path path) {
      ValidationUtils.requireAllPresent(
          "Section 'timeouts' is incomplete in " + path, viewChangeMs, retransmitMs, maxBackoffMs);
      return new TimeoutsSection(viewChangeMs, retransmitMs, maxBackoffMs);
    }
  }

  // Immutable parsed sections
  public record SystemSection(String name, String environment, int n, int f, String leaderElection, int baseView) {}

  public record ReplicaSection(String id, String host, int consensusPort, int clientPort, String publicKeyPath) {}

  public record ClientSection(String id, String host, int requestTimeoutMs, List<String> knownReplicas) {}

  public record TimeoutsSection(int viewChangeMs, int retransmitMs, int maxBackoffMs) {}
}
