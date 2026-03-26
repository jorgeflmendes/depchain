package pt.ulisboa.depchain.testsupport;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.config.ConfigParser.ReplicaSection;

public final class TestKeyMaterialSupport {
  private static final Object POPULATE_LOCK = new Object();
  private static final Object ISOLATED_CONFIG_LOCK = new Object();
  private static final Map<String, Path> ISOLATED_CONFIG_PATHS = new ConcurrentHashMap<>();
  private static final Pattern KEYS_ROOT_PATTERN = Pattern.compile("(?m)^(\\s*root:\\s+).*$");
  private static final Pattern BLOCKS_ROOT_PATTERN = Pattern.compile("(?m)^(\\s*blocksRoot:\\s+).*$");

  private TestKeyMaterialSupport() {
  }

  public static Path projectConfigPath() {
    return Path.of(System.getProperty("user.dir"), "config", "config.yaml").toAbsolutePath();
  }

  public static Path isolatedConfigPath(String scope) throws IOException {
    Path cachedPath = ISOLATED_CONFIG_PATHS.get(scope);
    if (cachedPath != null) {
      return cachedPath;
    }

    synchronized (ISOLATED_CONFIG_LOCK) {
      cachedPath = ISOLATED_CONFIG_PATHS.get(scope);
      if (cachedPath != null) {
        return cachedPath;
      }

      Path isolatedPath = createIsolatedConfig(scope);
      ISOLATED_CONFIG_PATHS.put(scope, isolatedPath);
      return isolatedPath;
    }
  }

  public static void ensureKeyMaterial(Path configPath) throws IOException, InterruptedException {
    ConfigParser config = ConfigParser.load(configPath);
    if (hasPopulateOutputs(configPath, config)) {
      return;
    }

    synchronized (POPULATE_LOCK) {
      config = ConfigParser.load(configPath);
      if (hasPopulateOutputs(configPath, config)) {
        return;
      }

      ProcessResult populateResult = runPopulate(configPath);
      if (populateResult.exitCode() != 0) {
        throw new IllegalStateException("Populate failed for " + configPath + System.lineSeparator() + populateResult.output());
      }
    }
  }

  private static boolean hasPopulateOutputs(Path configPath, ConfigParser config) {
    if (!Files.exists(GenesisPathSupport.genesisPathFor(configPath)) || !Files.exists(addressesPathFor(configPath))) {
      return false;
    }

    for (ConfigParser.ClientSection client : config.clients()) {
      if (!Files.exists(client.publicKeyPath()) || !Files.exists(client.privateKeyPath())) {
        return false;
      }
    }

    for (ReplicaSection replica : config.replicas()) {
      if (!Files.exists(replica.publicKeyPath()) || !Files.exists(replica.privateKeyPath()) || !Files.exists(replica.thresholdPublicKeyPath())
          || !Files.exists(replica.thresholdPrivateSharePath())) {
        return false;
      }
    }

    return true;
  }

  private static Path createIsolatedConfig(String scope) throws IOException {
    Path baseConfigPath = projectConfigPath();
    Path baseConfigDirectory = baseConfigPath.getParent();
    if (baseConfigDirectory == null) {
      throw new IllegalStateException("Project config must have a parent directory");
    }

    Path isolatedDirectory = Path.of(System.getProperty("user.dir"), "target", "test-configs", sanitizeScope(scope)).toAbsolutePath().normalize();
    resetDirectory(isolatedDirectory);

    Path isolatedRuntimeDirectory = isolatedDirectory.resolve("runtime");
    String configContents = Files.readString(baseConfigPath, StandardCharsets.UTF_8);
    configContents = replacePath(configContents, KEYS_ROOT_PATTERN, isolatedRuntimeDirectory.resolve("keys"));
    configContents = replacePath(configContents, BLOCKS_ROOT_PATTERN, isolatedRuntimeDirectory.resolve("storage"));

    Path isolatedConfigPath = isolatedDirectory.resolve("config.yaml");
    Files.writeString(isolatedConfigPath, configContents, StandardCharsets.UTF_8);
    Files.copy(baseConfigDirectory.resolve("genesis.json"), isolatedDirectory.resolve("genesis.json"), StandardCopyOption.REPLACE_EXISTING);
    return isolatedConfigPath;
  }

  private static String replacePath(String configContents, Pattern pattern, Path path) {
    String replacement = "$1" + "\"" + normalizePath(path) + "\"";
    String updated = pattern.matcher(configContents).replaceFirst(replacement);
    if (updated.equals(configContents)) {
      throw new IllegalStateException("Could not rewrite config path using pattern " + pattern);
    }
    return updated;
  }

  private static void resetDirectory(Path directory) throws IOException {
    if (Files.exists(directory)) {
      try (var paths = Files.walk(directory)) {
        paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
          try {
            Files.deleteIfExists(path);
          } catch (IOException exception) {
            throw new IllegalStateException("Could not reset test directory " + directory, exception);
          }
        });
      }
    }
    Files.createDirectories(directory);
  }

  private static Path addressesPathFor(Path configPath) {
    Path parent = configPath.toAbsolutePath().normalize().getParent();
    if (parent == null) {
      throw new IllegalArgumentException("configPath must have a parent directory");
    }
    return parent.resolve("addresses.json");
  }

  private static String sanitizeScope(String scope) {
    return scope.replaceAll("[^A-Za-z0-9._-]", "-");
  }

  private static String normalizePath(Path path) {
    return path.toAbsolutePath().normalize().toString().replace('\\', '/');
  }

  private static ProcessResult runPopulate(Path configPath) throws IOException, InterruptedException {
    ProcessBuilder processBuilder = new ProcessBuilder(javaExecutable(), "-cp", System.getProperty("java.class.path"), "pt.ulisboa.depchain.populate.Populate",
        configPath.toString());
    processBuilder.redirectErrorStream(true);

    Process process = processBuilder.start();
    String output;
    try (InputStream inputStream = process.getInputStream()) {
      output = readAll(inputStream);
    }
    int exitCode = process.waitFor();
    return new ProcessResult(exitCode, output);
  }

  private static String javaExecutable() {
    String javaHome = System.getProperty("java.home");
    String suffix = System.getProperty("os.name").toLowerCase().contains("win") ? ".exe" : "";
    return Path.of(javaHome, "bin", "java" + suffix).toString();
  }

  private static String readAll(InputStream inputStream) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    inputStream.transferTo(output);
    return output.toString(StandardCharsets.UTF_8);
  }

  private record ProcessResult(int exitCode, String output) {
  }

  private static final class GenesisPathSupport {
    private static Path genesisPathFor(Path configPath) {
      Path parent = configPath.toAbsolutePath().normalize().getParent();
      if (parent == null) {
        throw new IllegalArgumentException("configPath must have a parent directory");
      }
      return parent.resolve("genesis.json");
    }
  }
}
