package pt.ulisboa.depchain.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Tag("integration")
class ReplicaConnectivityTest {
  private static final List<String> REPLICA_IDS = List.of("server1", "server2", "server3", "server4");
  private static final Duration SERVER_READY_TIMEOUT = Duration.ofSeconds(15);

  @Test
  @Timeout(60)
  void clientCanReachAllReplicas() throws Exception {
    // Loads the integration config used by server and client processes.
    Path configPath = Path.of(System.getProperty("user.dir"), "config", "config.properties").toAbsolutePath();
    assertTrue(Files.exists(configPath), "Missing config file: " + configPath);

    // Starts all replica processes before sending client requests.
    List<ServerHandle> serverProcesses = new ArrayList<>();
    try {
      for (String replicaId : REPLICA_IDS) {
        serverProcesses.add(startServer(replicaId, configPath));
      }

      // Wait until each replica reports both listeners as ready before sending traffic.
      for (ServerHandle server : serverProcesses) {
        assertTrue(server.awaitReady(SERVER_READY_TIMEOUT), () -> "Replica %s did not become ready. Output:%n%s".formatted(server.replicaId(), server.output()));
      }

      // Sends one request to each replica and validates the returned response text.
      for (String replicaId : REPLICA_IDS) {
        String message = "smoke-" + replicaId;
        ProcessResult result = runClient(message, replicaId, configPath);

        assertEquals(0, result.exitCode(), "Client exited with error for " + replicaId + ": " + result.output());
        assertTrue(result.output().contains("response = Received " + message), "Unexpected client output for " + replicaId + ": " + result.output());
      }
    } finally {
      // Stops all spawned server processes even if assertions fail.
      stopProcesses(serverProcesses);
    }
  }

  private static ServerHandle startServer(String replicaId, Path configPath) throws IOException {
    // Launches a replica JVM process bound to the configured replica id.
    ProcessBuilder pb = new ProcessBuilder(javaExecutable(), "-cp", System.getProperty("java.class.path"), "pt.ulisboa.depchain.server.Main", replicaId, configPath.toString());
    pb.redirectErrorStream(true);

    Process process = pb.start();
    return new ServerHandle(replicaId, process);
  }

  private static ProcessResult runClient(String value, String targetReplicaId, Path configPath) throws IOException, InterruptedException {
    // Launches the interactive client JVM process and feeds one request followed by EXIT.
    ProcessBuilder pb = new ProcessBuilder(javaExecutable(), "-cp", System.getProperty("java.class.path"), "pt.ulisboa.depchain.client.Main", targetReplicaId,
        configPath.toString());
    pb.redirectErrorStream(true);

    Process process = pb.start();
    try (OutputStreamWriter writer = new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8)) {
      writer.write(value);
      writer.write(System.lineSeparator());
      writer.write("EXIT");
      writer.write(System.lineSeparator());
      writer.flush();
    }

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

  private static void stopProcesses(List<ServerHandle> processes) {
    // Requests graceful shutdown for all still-running processes.
    for (ServerHandle server : processes) {
      Process process = server.process();
      if (!process.isAlive()) {
        continue;
      }

      process.destroy();
    }

    // Force-kills any process that did not exit after a short grace period.
    for (ServerHandle server : processes) {
      Process process = server.process();
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

    for (ServerHandle server : processes) {
      server.joinReader();
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

  private record ProcessResult(int exitCode, String output) {
  }

  private static final class ServerHandle {
    private final String replicaId;
    private final Process process;
    private final StringBuilder output = new StringBuilder();
    private final Thread outputReader;

    private ServerHandle(String replicaId, Process process) {
      this.replicaId = replicaId;
      this.process = process;
      this.outputReader = Thread.ofVirtual().name("server-output-" + replicaId).start(this::captureOutput);
    }

    private String replicaId() {
      return replicaId;
    }

    private Process process() {
      return process;
    }

    private boolean awaitReady(Duration timeout) throws InterruptedException {
      long deadlineNanos = System.nanoTime() + timeout.toNanos();
      while (System.nanoTime() < deadlineNanos) {
        if (hasReadyMarkers()) {
          return true;
        }

        if (!process.isAlive()) {
          outputReader.join(TimeUnit.SECONDS.toMillis(1));
          return hasReadyMarkers();
        }

        Thread.sleep(100L);
      }

      return hasReadyMarkers();
    }

    private String output() {
      synchronized (output) {
        return output.toString();
      }
    }

    private void joinReader() {
      try {
        outputReader.join(TimeUnit.SECONDS.toMillis(1));
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
    }

    private boolean hasReadyMarkers() {
      String currentOutput = output();
      return currentOutput.contains("Replica " + replicaId + " client listener:") && currentOutput.contains("Replica " + replicaId + " node listener:");
    }

    private void captureOutput() {
      try (InputStream inputStream = process.getInputStream()) {
        byte[] buffer = new byte[1024];
        int bytesRead;
        while ((bytesRead = inputStream.read(buffer)) != -1) {
          synchronized (output) {
            output.append(new String(buffer, 0, bytesRead, StandardCharsets.UTF_8));
          }
        }
      } catch (IOException ignored) {
      }
    }
  }
}
