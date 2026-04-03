package pt.ulisboa.depchain.integration.byzantine;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pt.ulisboa.depchain.integration.support.IntegrationHarness.StartedServer;
import pt.ulisboa.depchain.server.node.BlockStore;
import pt.ulisboa.depchain.shared.config.ConfigParser;

@Tag("integration")
class ByzantineLeaderIntegrationTest extends AbstractByzantineIntegrationTest {
  @Test
  void invalidPrepareProposalQcTest() throws Exception {
    runLeaderAttackScenario(ByzantineAttackMode.INVALID_PREPARE_PROPOSAL_QC);
  }

  @Test
  void invalidPreCommitQcTest() throws Exception {
    runLeaderAttackScenario(ByzantineAttackMode.INVALID_PRE_COMMIT_QC);
  }

  @Test
  void invalidCommitQcTest() throws Exception {
    runLeaderAttackScenario(ByzantineAttackMode.INVALID_COMMIT_QC);
  }

  @Test
  void invalidDecideQcTest() throws Exception {
    runLeaderAttackScenario(ByzantineAttackMode.INVALID_DECIDE_QC);
  }

  @Test
  void equivocatingPrepareProposalTest() throws Exception {
    runLeaderAttackScenario(ByzantineAttackMode.EQUIVOCATING_PREPARE_PROPOSAL);
  }

  @Test
  void partialPrepareBroadcastTest() throws Exception {
    runLeaderAttackScenario(ByzantineAttackMode.PARTIAL_PREPARE_BROADCAST);
  }

  @Test
  void equivocatingLeaderStillAllowsSubsequentProgressAndHonestConvergence() throws Exception {
    Path configPath = integrationConfigPath();
    cleanPersistedBlockData(configPath);
    populateConfig(configPath);

    List<StartedServer> servers = new ArrayList<>(startServers(HONEST_WITH_BYZANTINE_LEADER_REPLICA_IDS, configPath));
    StartedServer byzantineLeader = startByzantineServer(LEADER_REPLICA_ID, configPath, ByzantineAttackMode.EQUIVOCATING_PREPARE_PROPOSAL);
    servers.add(byzantineLeader);
    try {
      waitForServersStartup(servers, STARTUP_TIMEOUT);
      assertRequestSucceeds(configPath, "equivocation-follow-up-1", VIEW_CHANGE_REQUEST_TIMEOUT, servers, "Cluster should decide the first request despite leader equivocation");
      assertRequestSucceeds(configPath, "equivocation-follow-up-2", VIEW_CHANGE_REQUEST_TIMEOUT, servers, "Cluster should keep making progress after leader equivocation");
      assertByzantineAttackObserved(byzantineLeader, ByzantineAttackMode.EQUIVOCATING_PREPARE_PROPOSAL, "Byzantine leader equivocation was never exercised");

      ConfigParser config = ConfigParser.load(configPath);
      await().forever().untilAsserted(() -> {
        Long expectedHeight = null;
        String expectedHash = null;
        for (String replicaId : HONEST_WITH_BYZANTINE_LEADER_REPLICA_IDS) {
          BlockStore.BlockDocument latest = BlockStore.forReplica(config, replicaId).loadLatest().orElseThrow();
          if (expectedHeight == null) {
            expectedHeight = latest.height();
            expectedHash = latest.blockHash();
          } else {
            assertEquals(expectedHeight, latest.height(), "Honest replicas should still agree on height after repeated progress under equivocation");
            assertEquals(expectedHash, latest.blockHash(), "Honest replicas should still agree on latest block after repeated progress under equivocation");
          }
        }
        assertEquals(2L, expectedHeight, "Two successful client requests should produce two decided blocks");
      });
    } finally {
      try {
        stopProcesses(servers);
      } finally {
        cleanPersistedBlockData(configPath);
      }
    }
  }
}
