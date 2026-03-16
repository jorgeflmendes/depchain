package pt.ulisboa.depchain.integration;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Tag("integration")
class MaliciousLeaderSecTest extends IntegrationTestSupport {
  @Test
  @Timeout(90)
  void maliciousLeaderInvalidPrepareProposalQcTest() throws Exception {
    runSingleMaliciousLeaderAttackScenario(ReplicaAttackMode.INVALID_PREPARE_PROPOSAL_QC);
  }

  @Test
  @Timeout(90)
  void maliciousLeaderInvalidPreCommitQcTest() throws Exception {
    runSingleMaliciousLeaderAttackScenario(ReplicaAttackMode.INVALID_PRE_COMMIT_QC);
  }

  @Test
  @Timeout(90)
  void maliciousLeaderInvalidCommitQcTest() throws Exception {
    runSingleMaliciousLeaderAttackScenario(ReplicaAttackMode.INVALID_COMMIT_QC);
  }

  @Test
  @Timeout(90)
  void maliciousLeaderInvalidDecideQcTest() throws Exception {
    runSingleMaliciousLeaderAttackScenario(ReplicaAttackMode.INVALID_DECIDE_QC);
  }

  private void runSingleMaliciousLeaderAttackScenario(ReplicaAttackMode attackMode) throws Exception {
    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    String scenarioLabel = "malicious leader with " + attackMode;
    List<StartedServer> servers = startServers(HONEST_WITH_BYZANTINE_LEADER_REPLICA_IDS, configPath);
    try {
      try (ByzantineReplicaHandle byzantineLeader = new ByzantineReplicaHandle(configPath, LEADER_REPLICA_ID, attackMode)) {
        waitForServersStartup(servers, Duration.ofSeconds(15));

        assertRequestSucceeds(configPath, FOLLOWER_REPLICA_ID, "malicious-leader-forwarded-" + attackMode.name().toLowerCase(), Duration.ofSeconds(30), servers, scenarioLabel
            + " should still allow progress after the honest replicas move to the next view");
        assertReplayIsIgnored(configPath, FOLLOWER_REPLICA_ID, "malicious-leader-replay-" + attackMode.name().toLowerCase(), servers, scenarioLabel
            + " should still preserve replay protection");
        org.junit.jupiter.api.Assertions.assertTrue(byzantineLeader.attackObserved(), scenarioLabel + " was never exercised");
      }
    } finally {
      stopProcesses(servers);
    }
  }
}
