package pt.ulisboa.depchain.integration;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.shared.utils.ProtoValidationUtil;

@Tag("integration")
class MaliciousReplicaSecTest extends IntegrationTestSupport {
  @Test
  @Timeout(120)
  void twoByzantineReplicasInvalidVoteTest() throws Exception {
    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    List<StartedServer> servers = startServers(HONEST_REPLICA_IDS, configPath);
    try {
      try (ByzantineReplicaHandle byzantineReplica3 = new ByzantineReplicaHandle(configPath, BYZANTINE_REPLICA_ID);
          ByzantineReplicaHandle byzantineReplica4 = new ByzantineReplicaHandle(configPath, SECOND_BYZANTINE_REPLICA_ID)) {
        waitForServersStartup(servers, Duration.ofSeconds(15));

        ClientRequest request = signedRequest(configPath, "byzantine-test");
        byte[] payload = ProtoValidationUtil.requireValid(request, "ClientRequest").toByteArray();
        var response = broadcastClientRequestPayload(configPath, payload, Duration.ofSeconds(10));
        assertNull(response, "Client request should time out when two Byzantine replicas prevent quorum");
        assertTrue(byzantineReplica3.attackObserved(), "Byzantine replica server3 was not exercised");
        assertTrue(byzantineReplica4.attackObserved(), "Byzantine replica server4 was not exercised");
      }
    } finally {
      stopProcesses(servers);
    }
  }

  @Test
  @Timeout(120)
  void maliciousReplicaDropsAllMessagesTest() throws Exception {
    runSingleMaliciousReplicaAttackScenario(ReplicaAttackMode.DROP_ALL_MESSAGES);
  }

  @Test
  @Timeout(120)
  void maliciousReplicaInvalidNewViewTest() throws Exception {
    runSingleMaliciousReplicaAttackScenario(ReplicaAttackMode.INVALID_NEW_VIEW);
  }

  @Test
  @Timeout(120)
  void maliciousReplicaSpoofedSenderIdTest() throws Exception {
    runSingleMaliciousReplicaAttackScenario(ReplicaAttackMode.SPOOFED_REPLICA_SENDER_ID);
  }

  @Test
  @Timeout(120)
  void maliciousReplicaInvalidPrepareVoteTest() throws Exception {
    runSingleMaliciousReplicaAttackScenario(ReplicaAttackMode.INVALID_PREPARE_VOTE);
  }

  @Test
  @Timeout(120)
  void maliciousReplicaInvalidPreCommitVoteTest() throws Exception {
    runSingleMaliciousReplicaAttackScenario(ReplicaAttackMode.INVALID_PRE_COMMIT_VOTE);
  }

  @Test
  @Timeout(120)
  void maliciousReplicaInvalidCommitVoteTest() throws Exception {
    runSingleMaliciousReplicaAttackScenario(ReplicaAttackMode.INVALID_COMMIT_VOTE);
  }

  @Test
  @Timeout(120)
  void maliciousReplicaStalePrepareVoteTest() throws Exception {
    runSingleMaliciousReplicaAttackScenario(ReplicaAttackMode.STALE_PREPARE_VOTE);
  }

  @Test
  @Timeout(120)
  void maliciousReplicaStalePreCommitVoteTest() throws Exception {
    runSingleMaliciousReplicaAttackScenario(ReplicaAttackMode.STALE_PRE_COMMIT_VOTE);
  }

  @Test
  @Timeout(120)
  void maliciousReplicaStaleCommitVoteTest() throws Exception {
    runSingleMaliciousReplicaAttackScenario(ReplicaAttackMode.STALE_COMMIT_VOTE);
  }

  private void runSingleMaliciousReplicaAttackScenario(ReplicaAttackMode attackMode) throws Exception {
    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    String scenarioLabel = "single malicious replica with " + attackMode;
    List<StartedServer> servers = startServers(HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS, configPath);
    try {
      try (ByzantineReplicaHandle byzantineReplica = new ByzantineReplicaHandle(configPath, BYZANTINE_REPLICA_ID, attackMode)) {
        waitForServersStartup(servers, Duration.ofSeconds(15));

        assertRequestSucceeds(configPath, "malicious-leader-" + attackMode.name().toLowerCase(), Duration.ofSeconds(60), servers, scenarioLabel
            + " should preserve leader-path liveness");
        assertRequestSucceeds(configPath, "malicious-broadcast-" + attackMode.name().toLowerCase(), Duration.ofSeconds(60), servers, scenarioLabel
            + " should preserve broadcast-path liveness");
        assertReplayIsIgnored(configPath, "malicious-replay-" + attackMode.name().toLowerCase(), servers, scenarioLabel + " should preserve replay protection");
        assertTrue(byzantineReplica.attackObserved(), scenarioLabel + " was never exercised");
      }
    } finally {
      stopProcesses(servers);
    }
  }
}
