package pt.ulisboa.depchain.integration.cluster;

import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import pt.ulisboa.depchain.integration.support.IntegrationHarness;
import pt.ulisboa.depchain.integration.support.IntegrationHarness.ManagedCluster;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HonestClusterIntegrationTest extends IntegrationHarness {
  private ManagedCluster sharedCluster;

  @BeforeAll
  void startSharedCluster() throws Exception {
    sharedCluster = startManagedCluster(REPLICA_IDS);
  }

  @AfterAll
  void stopSharedCluster() throws Exception {
    if (sharedCluster != null) {
      sharedCluster.close();
    }
  }

  @Test
  @Timeout(30)
  void normalExecutionTest() throws Exception {
    for (int i = 1; i <= 4; i++) {
      sharedCluster.assertRequestSucceeds("simple-test-" + i, STANDARD_REQUEST_TIMEOUT, "Client request should receive a response");
    }
  }

  @Test
  @Timeout(30)
  void broadcastClientRequestTest() throws Exception {
    sharedCluster.assertRequestSucceeds("broadcast-test", STANDARD_REQUEST_TIMEOUT, "Broadcast client request should receive a response");
  }

  @Test
  @Timeout(30)
  void replayedClientRequestTest() throws Exception {
    sharedCluster.assertReplayIsIgnored("replayed-test", "Replayed client request should not receive a response");
  }

  @Test
  @Timeout(30)
  void forgedClientSignatureTest() throws Exception {
    InboundPacket response = sharedCluster.sendForgedClientRequest(LEADER_REPLICA_ID, "forged-test");
    assertNull(response, "Forged client request should not receive a response");
  }

  @Test
  @Timeout(45)
  void followerCrashTest() throws Exception {
    try (ManagedCluster cluster = startManagedCluster(REPLICA_IDS)) {
      cluster.assertRequestSucceeds("before-follower-crash", STANDARD_REQUEST_TIMEOUT, "Client request should succeed before follower crash");

      stopProcess(cluster.servers().get(2));

      cluster.assertRequestSucceeds("after-follower-crash", VIEW_CHANGE_REQUEST_TIMEOUT, "Client request should still succeed after a follower crashes");
    }
  }

  @Test
  @Timeout(60)
  void leaderCrashTest() throws Exception {
    try (ManagedCluster cluster = startManagedCluster(REPLICA_IDS)) {
      cluster.assertRequestSucceeds("before-leader-crash", STANDARD_REQUEST_TIMEOUT, "Client request should succeed before leader crash");

      stopProcess(cluster.servers().getFirst());

      cluster.assertRequestSucceeds("after-leader-crash", VIEW_CHANGE_REQUEST_TIMEOUT, "Client request should still succeed after the leader crashes");
    }
  }
}
