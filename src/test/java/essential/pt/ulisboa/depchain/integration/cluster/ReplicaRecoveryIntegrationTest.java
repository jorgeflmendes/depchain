package pt.ulisboa.depchain.integration.cluster;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import pt.ulisboa.depchain.client.api.ClientReplicaApi;
import pt.ulisboa.depchain.integration.support.IntegrationHarness;
import pt.ulisboa.depchain.integration.support.IntegrationHarness.StartedServer;
import pt.ulisboa.depchain.server.node.BlockStore;
import pt.ulisboa.depchain.shared.config.ConfigParser;

@Tag("integration")
class ReplicaRecoveryIntegrationTest extends IntegrationHarness {
  private static final String CLIENT_ID = "client";

  @Test
  @Timeout(120)
  void restartedFollowerCatchesUpAfterMissingMultipleBlocks() throws Exception {
    Path configPath = integrationConfigPath();
    cleanPersistedBlockData(configPath);
    populateConfig(configPath);

    List<StartedServer> servers = new ArrayList<>(startServers(REPLICA_IDS, configPath));
    StartedServer stoppedFollower = servers.stream().filter(server -> FOLLOWER_REPLICA_ID.equals(server.replicaId())).findFirst().orElseThrow();

    try {
      waitForServersStartup(servers, STARTUP_TIMEOUT);

      try (ClientReplicaApi client = ClientReplicaApi.connect(configPath.toString(), CLIENT_ID)) {
        assertEquals(true, client.transferDepCoin(TEST_RECIPIENT_ADDRESS, 3L, 0L, TEST_GAS_LIMIT, TEST_GAS_PRICE).getReceipt().getSuccess());
      }

      stopProcess(stoppedFollower);
      servers.remove(stoppedFollower);

      try (ClientReplicaApi client = ClientReplicaApi.connect(configPath.toString(), CLIENT_ID)) {
        assertEquals(true, client.transferDepCoin(TEST_RECIPIENT_ADDRESS, 5L, 1L, TEST_GAS_LIMIT, TEST_GAS_PRICE).getReceipt().getSuccess());
        assertEquals(true, client.transferDepCoin(TEST_RECIPIENT_ADDRESS, 7L, 2L, TEST_GAS_LIMIT, TEST_GAS_PRICE).getReceipt().getSuccess());
      }

      StartedServer restartedFollower = startServers(List.of(FOLLOWER_REPLICA_ID), configPath).getFirst();
      servers.add(restartedFollower);
      waitForServersStartup(List.of(restartedFollower), Duration.ofSeconds(35));

      try (ClientReplicaApi client = ClientReplicaApi.connect(configPath.toString(), CLIENT_ID)) {
        assertEquals(true, client.transferDepCoin(TEST_RECIPIENT_ADDRESS, 11L, 3L, TEST_GAS_LIMIT, TEST_GAS_PRICE).getReceipt().getSuccess());
      }

      await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertReplicaHeightsAndBalancesConverged(configPath));
    } finally {
      try {
        stopProcesses(servers);
      } finally {
        try {
          cleanPersistedBlockData(configPath);
        } finally {
          cleanupIsolatedConfigDirectory(configPath);
        }
      }
    }
  }

  private static void assertReplicaHeightsAndBalancesConverged(Path configPath) throws Exception {
    ConfigParser config = ConfigParser.load(configPath);
    Long expectedHeight = null;
    String expectedBalance = null;
    for (String replicaId : REPLICA_IDS) {
      BlockStore.BlockDocument latest = BlockStore.forReplica(config, replicaId).loadLatest().orElseThrow(() -> new AssertionError("Missing latest block for " + replicaId));
      if (expectedHeight == null) {
        expectedHeight = latest.height();
        expectedBalance = latest.state().get(TEST_RECIPIENT_ADDRESS).balance();
      } else {
        assertEquals(expectedHeight, latest.height(), "Replica " + replicaId + " should catch up to the cluster height");
        assertEquals(expectedBalance, latest.state().get(TEST_RECIPIENT_ADDRESS).balance(), "Replica " + replicaId + " should converge to the same state");
      }
    }
    assertEquals("26", expectedBalance, "Recovered follower should converge to the cumulative transferred balance");
  }
}
