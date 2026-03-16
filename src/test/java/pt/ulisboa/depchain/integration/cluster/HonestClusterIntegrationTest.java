package pt.ulisboa.depchain.integration.cluster;

import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import pt.ulisboa.depchain.integration.support.IntegrationHarness;
import pt.ulisboa.depchain.integration.support.IntegrationHarness.StartedServer;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;

@Tag("integration")
class HonestClusterIntegrationTest extends IntegrationHarness {
  @Test
  @Timeout(30)
  void normalExecutionTest() throws Exception {
    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    List<StartedServer> servers = startServers(REPLICA_IDS, configPath);
    try {
      waitForServersStartup(servers, STARTUP_TIMEOUT);
      for (int i = 1; i <= 4; i++) {
        assertRequestSucceeds(configPath, "simple-test-" + i, STANDARD_REQUEST_TIMEOUT, servers, "Client request should receive a response");
      }
    } finally {
      stopProcesses(servers);
    }
  }

  @Test
  @Timeout(30)
  void broadcastClientRequestTest() throws Exception {
    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    List<StartedServer> servers = startServers(REPLICA_IDS, configPath);
    try {
      waitForServersStartup(servers, STARTUP_TIMEOUT);
      assertRequestSucceeds(configPath, "broadcast-test", STANDARD_REQUEST_TIMEOUT, servers, "Broadcast client request should receive a response");
    } finally {
      stopProcesses(servers);
    }
  }

  @Test
  @Timeout(30)
  void replayedClientRequestTest() throws Exception {
    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    List<StartedServer> servers = startServers(REPLICA_IDS, configPath);
    try {
      waitForServersStartup(servers, STARTUP_TIMEOUT);
      assertReplayIsIgnored(configPath, "replayed-test", servers, "Replayed client request should not receive a response");
    } finally {
      stopProcesses(servers);
    }
  }

  @Test
  @Timeout(30)
  void forgedClientSignatureTest() throws Exception {
    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    List<StartedServer> servers = startServers(REPLICA_IDS, configPath);
    try {
      waitForServersStartup(servers, STARTUP_TIMEOUT);
      InboundPacket response = sendForgedClientRequest(configPath, LEADER_REPLICA_ID, "forged-test");
      assertNull(response, "Forged client request should not receive a response");
    } finally {
      stopProcesses(servers);
    }
  }

  @Test
  @Timeout(45)
  void followerCrashTest() throws Exception {
    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    List<StartedServer> servers = startServers(REPLICA_IDS, configPath);
    try {
      waitForServersStartup(servers, STARTUP_TIMEOUT);
      assertRequestSucceeds(configPath, "before-follower-crash", STANDARD_REQUEST_TIMEOUT, servers, "Client request should succeed before follower crash");

      stopProcess(servers.get(2));

      assertRequestSucceeds(configPath, "after-follower-crash", VIEW_CHANGE_REQUEST_TIMEOUT, servers, "Client request should still succeed after a follower crashes");
    } finally {
      stopProcesses(servers);
    }
  }

  @Test
  @Timeout(60)
  void leaderCrashTest() throws Exception {
    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    List<StartedServer> servers = startServers(REPLICA_IDS, configPath);
    try {
      waitForServersStartup(servers, STARTUP_TIMEOUT);
      assertRequestSucceeds(configPath, "before-leader-crash", STANDARD_REQUEST_TIMEOUT, servers, "Client request should succeed before leader crash");

      stopProcess(servers.getFirst());

      assertRequestSucceeds(configPath, "after-leader-crash", VIEW_CHANGE_REQUEST_TIMEOUT, servers, "Client request should still succeed after the leader crashes");
    } finally {
      stopProcesses(servers);
    }
  }
}
