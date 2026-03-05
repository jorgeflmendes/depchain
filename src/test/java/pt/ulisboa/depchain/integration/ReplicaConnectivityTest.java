package pt.ulisboa.depchain.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Tag("integration")
class ReplicaConnectivityTest {
  private static final List<String> REPLICA_IDS = List.of("server1", "server2", "server3", "server4");

  @Test
  @Timeout(60)
  void clientCanReachAllReplicas() throws Exception {
    // Loads the integration config used by server and client processes.
    Path configPath = Path.of(System.getProperty("user.dir"), "config", "config.yaml").toAbsolutePath();
    assertTrue(Files.exists(configPath), "Missing config file: " + configPath);

    // Starts all replica processes before sending client requests.
    List<Process> serverProcesses = new ArrayList<>();
    try {
      for (String replicaId : REPLICA_IDS) {
        serverProcesses.add(startServer(replicaId, configPath));
      }

      // Give processes a moment to bind sockets before first client request.
      Thread.sleep(1200);

      // Sends one request to each replica and validates the returned response text.
      for (String replicaId : REPLICA_IDS) {
        String message = "smoke-" + replicaId;
        ProcessResult result = runClient(message, replicaId, configPath);

        assertEquals(0, result.exitCode(), "Client exited with error for " + replicaId + ": " + result.output());
        assertTrue(
            result.output().contains("response = Received " + message),
            "Unexpected client output for " + replicaId + ": " + result.output());
      }
    } finally {
      // Stops all spawned server processes even if assertions fail.
      stopProcesses(serverProcesses);
    }
  }

  private static Process startServer(String replicaId, Path configPath) throws IOException {
    // Launches a replica JVM process bound to the configured replica id.
    ProcessBuilder pb = new ProcessBuilder(javaExecutable(), "-cp", System.getProperty("java.class.path"), "pt.ulisboa.depchain.server.Main", replicaId, configPath.toString());
    pb.redirectErrorStream(true);
    
    return pb.start();
  }

  private static ProcessResult runClient(String value, String targetReplicaId, Path configPath) throws IOException, InterruptedException {
    // Launches a client JVM process that sends a value to one target replica.
    ProcessBuilder pb = new ProcessBuilder(javaExecutable(), "-cp", System.getProperty("java.class.path"), "pt.ulisboa.depchain.client.Main", value, targetReplicaId, configPath.toString());
    pb.redirectErrorStream(true);

    Process process = pb.start();

    // Waits for the client process to finish within a fixed timeout.
    boolean finished = process.waitFor(Duration.ofSeconds(10).toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
    
    if (!finished) {
      process.destroyForcibly();
      return new ProcessResult(124, "Client timeout");
    }

    // Captures client stdout/stderr for assertion and diagnostics.
    String output = readAll(process.getInputStream());
    return new ProcessResult(process.exitValue(), output);
  }

  private static void stopProcesses(List<Process> processes) {
    // Requests graceful shutdown for all still-running processes.
    for (Process process : processes) {
      if (!process.isAlive()) {
        continue;
      }

      process.destroy();
    }

    // Force-kills any process that did not exit after a short grace period.
    for (Process process : processes) {
      if (!process.isAlive()) {
        continue;
      }
      try {
        if (!process.waitFor(2, java.util.concurrent.TimeUnit.SECONDS)) {
          process.destroyForcibly();
        }
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        process.destroyForcibly();
      }
    }
  }

  private static String javaExecutable() {
    // Resolves the current Java binary path across OSes.
    String javaHome = System.getProperty("java.home");
    String suffix = "";
    if (System.getProperty("os.name").toLowerCase().contains("win")) {
      suffix = ".exe";
    }
    return Path.of(javaHome, "bin", "java" + suffix).toString();
  }

  private static String readAll(InputStream inputStream) throws IOException {
    // Reads the full process output stream as UTF-8 text.
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    inputStream.transferTo(output);
    return output.toString(StandardCharsets.UTF_8);
  }

  private record ProcessResult(int exitCode, String output) {}
}
