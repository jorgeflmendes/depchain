package pt.ulisboa.depchain.testsupport;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.config.ConfigParser.ReplicaSection;

public final class TestKeyMaterialSupport {
  private static final Object POPULATE_LOCK = new Object();

  private TestKeyMaterialSupport() {
  }

  public static void ensureKeyMaterial(Path configPath) throws IOException, InterruptedException {
    ConfigParser config = ConfigParser.load(configPath);
    if (hasKeyMaterial(config)) {
      return;
    }

    synchronized (POPULATE_LOCK) {
      config = ConfigParser.load(configPath);
      if (hasKeyMaterial(config)) {
        return;
      }

      ProcessResult populateResult = runPopulate(configPath);
      if (populateResult.exitCode() != 0) {
        throw new IllegalStateException("Populate failed for " + configPath + System.lineSeparator() + populateResult.output());
      }
    }
  }

  private static boolean hasKeyMaterial(ConfigParser config) {
    if (!Files.exists(config.client().publicKeyPath()) || !Files.exists(config.client().privateKeyPath())) {
      return false;
    }

    for (ReplicaSection replica : config.replicas()) {
      if (!Files.exists(replica.publicKeyPath()) || !Files.exists(replica.privateKeyPath()) || !Files.exists(replica.thresholdPublicKeyPath())
          || !Files.exists(replica.thresholdPrivateSharePath())) {
        return false;
      }
    }

    return true;
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
}
