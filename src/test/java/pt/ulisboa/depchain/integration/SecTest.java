package pt.ulisboa.depchain.integration;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import pt.ulisboa.depchain.shared.network.model.InboundPacket;

@Tag("integration")
class SecTest extends IntegrationTestSupport {
  @Test
  @Timeout(60)
  void normalExecutionTest() throws Exception {
    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    List<StartedServer> servers = startServers(REPLICA_IDS, configPath);
    try {
      waitForServersStartup(servers, Duration.ofSeconds(10));
      for (int i = 1; i <= 4; i++) {
        assertRequestSucceeds(configPath, LEADER_REPLICA_ID, "simple-test-" + i, Duration.ofSeconds(45), servers, "Client request should receive a response");
      }
    } finally {
      stopProcesses(servers);
    }
  }

  @Test
  @Timeout(90)
  void forwardedClientRequestTest() throws Exception {
    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    List<StartedServer> servers = startServers(REPLICA_IDS, configPath);
    try {
      waitForServersStartup(servers, Duration.ofSeconds(15));
      assertRequestSucceeds(configPath, FOLLOWER_REPLICA_ID, "forwarded-test", Duration.ofSeconds(45), servers, "Forwarded client request should receive a response");
    } finally {
      stopProcesses(servers);
    }
  }

  @Test
  @Timeout(60)
  void replayedClientRequestTest() throws Exception {
    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    List<StartedServer> servers = startServers(REPLICA_IDS, configPath);
    try {
      waitForServersStartup(servers, Duration.ofSeconds(15));
      assertReplayIsIgnored(configPath, LEADER_REPLICA_ID, "replayed-test", servers, "Replayed client request should not receive a response");
    } finally {
      stopProcesses(servers);
    }
  }

  @Test
  @Timeout(60)
  void forgedClientSignatureTest() throws Exception {
    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    List<StartedServer> servers = startServers(REPLICA_IDS, configPath);
    try {
      waitForServersStartup(servers, Duration.ofSeconds(15));
      InboundPacket response = sendForgedClientRequest(configPath, LEADER_REPLICA_ID, "forged-test");
      org.junit.jupiter.api.Assertions.assertNull(response, "Forged client request should not receive a response");
    } finally {
      stopProcesses(servers);
    }
  }
}
