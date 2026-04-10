package pt.ulisboa.depchain.integration.cluster;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pt.ulisboa.depchain.integration.support.IntegrationHarness;
import pt.ulisboa.depchain.integration.support.IntegrationHarness.ManagedCluster;
import pt.ulisboa.depchain.integration.support.IntegrationHarness.StartedServer;
import pt.ulisboa.depchain.server.node.BlockStore;
import pt.ulisboa.depchain.shared.config.ConfigParser;

@Tag("integration")
class PersistenceFailureIntegrationTest extends IntegrationHarness {

  @Test
  void leaderCanReplyBeforeLocalPersistenceAndCatchUpAfterRestart() throws Exception {
    Map<String, Map<String, String>> perReplicaProperties = Map.of(LEADER_REPLICA_ID, Map.of(BlockStore.FAIL_AFTER_TEMP_WRITE_HEIGHT_PROPERTY, "1"));

    try (ManagedCluster cluster = startManagedClusterWithPerReplicaJvmProperties(REPLICA_IDS, perReplicaProperties)) {
      ConfigParser config = ConfigParser.load(cluster.configPath());
      StartedServer failedLeader = cluster.servers().stream().filter(server -> LEADER_REPLICA_ID.equals(server.replicaId())).findFirst().orElseThrow();

      cluster
          .assertRequestSucceeds("leader-replies-before-local-persist", STANDARD_REQUEST_TIMEOUT, "Client should still observe success even if the leader fails local persistence after execution");

      await().forever().untilAsserted(() -> {
        assertEquals(0L, BlockStore.forReplica(config, LEADER_REPLICA_ID).loadLatest().orElseThrow().height());
        assertEquals(1L, BlockStore.forReplica(config, FOLLOWER_REPLICA_ID).loadLatest().orElseThrow().height());
        assertEquals(1L, BlockStore.forReplica(config, BYZANTINE_REPLICA_ID).loadLatest().orElseThrow().height());
        assertEquals(1L, BlockStore.forReplica(config, SECOND_BYZANTINE_REPLICA_ID).loadLatest().orElseThrow().height());
      });

      stopProcess(failedLeader);
      List<StartedServer> liveServers = new ArrayList<>(cluster.servers());
      liveServers.remove(failedLeader);

      StartedServer restartedLeader = startServers(List.of(LEADER_REPLICA_ID), cluster.configPath()).getFirst();
      liveServers.add(restartedLeader);
      waitForServersStartup(List.of(restartedLeader), STARTUP_TIMEOUT);

      cluster
          .assertRequestSucceeds("post-leader-restart-after-persist-failure", STANDARD_REQUEST_TIMEOUT, "Cluster should continue after restarting the leader that missed local persistence");
      awaitPersistedHeight(config, FOLLOWER_REPLICA_ID, 2L);
      cluster
          .assertRequestSucceeds("second-post-leader-restart-after-persist-failure", VIEW_CHANGE_REQUEST_TIMEOUT, "Cluster should keep making progress while the restarted leader aligns its view");
      awaitPersistedHeight(config, FOLLOWER_REPLICA_ID, 3L);
      cluster
          .assertRequestSucceeds("third-post-leader-restart-after-persist-failure", VIEW_CHANGE_REQUEST_TIMEOUT, "Cluster should keep making progress while the restarted leader catches up");
      awaitPersistedHeight(config, FOLLOWER_REPLICA_ID, 4L);

      try {
        await().forever().untilAsserted(() -> {
          long expectedHeight = BlockStore.forReplica(config, FOLLOWER_REPLICA_ID).loadLatest().orElseThrow().height();
          for (String replicaId : REPLICA_IDS) {
            assertEquals(expectedHeight, BlockStore.forReplica(config, replicaId).loadLatest().orElseThrow().height(), "Replica " + replicaId
                + " should converge after the restarted leader catches up");
          }
        });
      } catch (Throwable failure) {
        fail(clusterDiagnostics("Leader persistence recovery did not converge", liveServers), failure);
      }
    }
  }

  private static String clusterDiagnostics(String header, List<StartedServer> servers) {
    StringBuilder diagnostics = new StringBuilder(header).append(System.lineSeparator());
    for (StartedServer server : servers) {
      diagnostics.append(server.describeState()).append(System.lineSeparator());
    }
    return diagnostics.toString();
  }

  private static void awaitPersistedHeight(ConfigParser config, String replicaId, long expectedHeight) {
    await().forever().untilAsserted(() -> assertEquals(expectedHeight, BlockStore.forReplica(config, replicaId).loadLatest().orElseThrow().height(), "Replica " + replicaId
        + " should persist the next block before the next recovery step"));
  }
}
