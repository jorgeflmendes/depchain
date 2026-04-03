package pt.ulisboa.depchain.integration.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import pt.ulisboa.depchain.integration.support.ClusterIntegrationTestBase;
import pt.ulisboa.depchain.integration.support.IntegrationHarness.ManagedCluster;
import pt.ulisboa.depchain.integration.support.IntegrationHarness.StartedServer;
import pt.ulisboa.depchain.server.node.BlockStore;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;

@Tag("integration")
@DisplayName("Honest cluster behaviour")
class HonestClusterIntegrationTest extends ClusterIntegrationTestBase {
  @Nested
  @DisplayName("Happy path")
  class HappyPath {
    @ParameterizedTest(name = "request {0} completes successfully")
    @ValueSource(ints = {1, 2, 3, 4})
    void normalExecutionTest(int requestIndex) throws Exception {
      cluster().assertRequestSucceeds("simple-test-" + requestIndex, STANDARD_REQUEST_TIMEOUT, "Client request should receive a response");
    }

    @Test
    @DisplayName("broadcast client requests complete successfully")
    void broadcastClientRequestTest() throws Exception {
      cluster().assertRequestSucceeds("broadcast-test", STANDARD_REQUEST_TIMEOUT, "Broadcast client request should receive a response");
    }

    @Test
    @DisplayName("replayed client requests are ignored")
    void replayedClientRequestTest() throws Exception {
      cluster().assertReplayIsIgnored("replayed-test", "Replayed client request should not receive a response");
    }

    @Test
    @DisplayName("forged client signatures are rejected")
    void forgedClientSignatureTest() throws Exception {
      InboundPacket response = cluster().sendForgedClientRequest(LEADER_REPLICA_ID, "forged-test");
      assertThat(response).isNull();
    }
  }

  @Nested
  @DisplayName("Crash recovery")
  class CrashRecovery {
    @Test
    @DisplayName("cluster remains live after follower crash")
    void followerCrashTest() throws Exception {
      try (ManagedCluster isolatedCluster = startManagedCluster(REPLICA_IDS)) {
        isolatedCluster.assertRequestSucceeds("before-follower-crash", STANDARD_REQUEST_TIMEOUT, "Client request should succeed before follower crash");
        awaitClusterReplicatedHeight(isolatedCluster, 1L);
        StartedServer crashedFollower = isolatedCluster.servers().get(2);
        stopProcess(crashedFollower);
        isolatedCluster.assertRequestSucceeds("after-follower-crash", VIEW_CHANGE_REQUEST_TIMEOUT, "Client request should still succeed after a follower crashes");
      }
    }

    @Test
    @DisplayName("cluster remains live after leader crash")
    void leaderCrashTest() throws Exception {
      try (ManagedCluster isolatedCluster = startManagedCluster(REPLICA_IDS)) {
        isolatedCluster.assertRequestSucceeds("before-leader-crash", STANDARD_REQUEST_TIMEOUT, "Client request should succeed before leader crash");
        awaitClusterReplicatedHeight(isolatedCluster, 1L);
        StartedServer crashedLeader = isolatedCluster.servers().getFirst();
        stopProcess(crashedLeader);
        isolatedCluster.assertRequestSucceeds("after-leader-crash", VIEW_CHANGE_REQUEST_TIMEOUT, "Client request should still succeed after the leader crashes");
      }
    }
  }

  private static void awaitClusterReplicatedHeight(ManagedCluster cluster, long expectedHeight) throws Exception {
    ConfigParser config = ConfigParser.load(cluster.configPath());
    awaitFor("cluster height " + expectedHeight).forever().untilAsserted(() -> {
      for (String replicaId : REPLICA_IDS) {
        long actualHeight = BlockStore.forReplica(config, replicaId).loadLatest().orElseThrow(() -> new AssertionError("Missing latest block for " + replicaId)).height();
        assertThat(actualHeight).as("replica %s persisted height", replicaId).isEqualTo(expectedHeight);
      }
    });
  }
}
