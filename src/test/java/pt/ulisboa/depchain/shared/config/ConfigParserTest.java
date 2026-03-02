package pt.ulisboa.depchain.shared.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConfigParserTest {
  @TempDir
  Path tempDir;

  @Test
  void loadRejectsReplicaWithoutPublicKeyPath() throws Exception {
    Path configPath = tempDir.resolve("config.yaml");
    Files.writeString(configPath, configWithoutPublicKeyPath(), StandardCharsets.UTF_8);

    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> ConfigParser.load(configPath));

    assertTrue(error.getMessage().contains("Replica entry is incomplete"));
  }

  @Test
  void loadParsesValidConfiguration() throws Exception {
    Path configPath = tempDir.resolve("config.yaml");
    Files.writeString(configPath, validConfig(), StandardCharsets.UTF_8);

    ConfigParser config = ConfigParser.load(configPath);

    assertEquals(4, config.system().n());
    assertEquals(4, config.replicas().size());
    assertEquals("server1", config.replicas().getFirst().id());
    assertEquals(100L, config.stubborn().baseDelayMs());
    assertEquals(5_000L, config.stubborn().maxDelayMs());
    assertEquals(0.20d, config.stubborn().jitterRatio());
    assertEquals(50_000, config.stubborn().maxPending());
    assertEquals(1_024, config.stubborn().heapCompactMinSize());
    assertEquals(12, config.stubborn().maxRetryAttempts());
    assertEquals(30_000L, config.stubborn().maxTrackedLifetimeMs());
    assertEquals(1_000, config.perfect().maxWindowSize());
    assertEquals(4_096, config.perfect().maxStreamStates());
    assertEquals(60_000L, config.perfect().streamIdleTtlMs());
  }

  private static String configWithoutPublicKeyPath() {
    return """
        system:
          name: depchain
          environment: test
          n: 4
          f: 1
          leaderElection: round-robin
          baseView: 1

        replicas:
          - id: server1
            host: 127.0.0.1
            consensusPort: 10001
            clientPort: 11001
          - id: server2
            host: 127.0.0.1
            consensusPort: 10002
            clientPort: 11002
            publicKeyPath: config/keys/server2/public/replica.pem
          - id: server3
            host: 127.0.0.1
            consensusPort: 10003
            clientPort: 11003
            publicKeyPath: config/keys/server3/public/replica.pem
          - id: server4
            host: 127.0.0.1
            consensusPort: 10004
            clientPort: 11004
            publicKeyPath: config/keys/server4/public/replica.pem

        client:
          id: client
          host: 127.0.0.1
          requestTimeoutMs: 3000
          knownReplicas:
            - server1
            - server2
            - server3
            - server4

        timeouts:
          viewChangeMs: 1500
          retransmitMs: 250
          maxBackoffMs: 10000

        stubborn:
          baseDelayMs: 100
          maxDelayMs: 5000
          jitterRatio: 0.20
          maxPending: 50000
          heapCompactMinSize: 1024
          maxRetryAttempts: 12
          maxTrackedLifetimeMs: 30000

        perfect:
          maxWindowSize: 1000
          maxStreamStates: 4096
          streamIdleTtlMs: 60000

        network:
          maxPacketSize: 8192
        """;
  }

  private static String validConfig() {
    return """
        system:
          name: depchain
          environment: test
          n: 4
          f: 1
          leaderElection: round-robin
          baseView: 1

        replicas:
          - id: server1
            host: 127.0.0.1
            consensusPort: 10001
            clientPort: 11001
            publicKeyPath: config/keys/server1/public/replica.pem
          - id: server2
            host: 127.0.0.1
            consensusPort: 10002
            clientPort: 11002
            publicKeyPath: config/keys/server2/public/replica.pem
          - id: server3
            host: 127.0.0.1
            consensusPort: 10003
            clientPort: 11003
            publicKeyPath: config/keys/server3/public/replica.pem
          - id: server4
            host: 127.0.0.1
            consensusPort: 10004
            clientPort: 11004
            publicKeyPath: config/keys/server4/public/replica.pem

        client:
          id: client
          host: 127.0.0.1
          requestTimeoutMs: 3000
          knownReplicas:
            - server1
            - server2
            - server3
            - server4

        timeouts:
          viewChangeMs: 1500
          retransmitMs: 250
          maxBackoffMs: 10000

        stubborn:
          baseDelayMs: 100
          maxDelayMs: 5000
          jitterRatio: 0.20
          maxPending: 50000
          heapCompactMinSize: 1024
          maxRetryAttempts: 12
          maxTrackedLifetimeMs: 30000

        perfect:
          maxWindowSize: 1000
          maxStreamStates: 4096
          streamIdleTtlMs: 60000

        network:
          maxPacketSize: 8192
        """;
  }
}
