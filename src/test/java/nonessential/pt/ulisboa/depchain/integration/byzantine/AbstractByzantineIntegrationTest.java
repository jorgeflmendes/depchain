package pt.ulisboa.depchain.integration.byzantine;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

import pt.ulisboa.depchain.integration.support.IntegrationHarness;
import pt.ulisboa.depchain.integration.support.IntegrationHarness.StartedServer;
import pt.ulisboa.depchain.server.node.BlockStore;
import pt.ulisboa.depchain.shared.config.ConfigParser;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractByzantineIntegrationTest extends IntegrationHarness {
  private Path sharedByzantineConfigPath;

  @BeforeAll
  void prepareByzantineConfig() throws Exception {
    sharedByzantineConfigPath = integrationConfigPath();
    cleanPersistedBlockData(sharedByzantineConfigPath);
    populateConfig(sharedByzantineConfigPath);
  }

  @AfterAll
  void cleanupByzantineConfig() throws Exception {
    if (sharedByzantineConfigPath != null) {
      try {
        cleanPersistedBlockData(sharedByzantineConfigPath);
      } finally {
        cleanupIsolatedConfigDirectory(sharedByzantineConfigPath);
      }
    }
  }

  protected final void runReplicaAttackScenario(ByzantineAttackMode attackMode) throws Exception {
    Path configPath = sharedByzantineConfigPath;
    cleanPersistedBlockData(configPath);

    List<StartedServer> servers = new ArrayList<>(startServers(HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS, configPath));
    StartedServer byzantineReplica = startByzantineServer(BYZANTINE_REPLICA_ID, configPath, attackMode);
    servers.add(byzantineReplica);
    try {
      waitForServersStartup(servers, STARTUP_TIMEOUT);
      assertRequestSucceeds(configPath, scenarioCommand("byzantine-replica", attackMode), VIEW_CHANGE_REQUEST_TIMEOUT, servers, "Single Byzantine replica should not break cluster liveness for "
          + attackMode);
      assertHonestReplicasConverged(configPath, HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS);
      assertByzantineAttackObserved(byzantineReplica, attackMode, "Byzantine replica attack was never exercised");
    } finally {
      try {
        stopProcesses(servers);
      } finally {
        cleanPersistedBlockData(configPath);
      }
    }
  }

  protected final void runLeaderAttackScenario(ByzantineAttackMode attackMode) throws Exception {
    Path configPath = sharedByzantineConfigPath;
    cleanPersistedBlockData(configPath);

    List<StartedServer> servers = new ArrayList<>(startServers(HONEST_WITH_BYZANTINE_LEADER_REPLICA_IDS, configPath));
    StartedServer byzantineLeader = startByzantineServer(LEADER_REPLICA_ID, configPath, attackMode);
    servers.add(byzantineLeader);
    try {
      waitForServersStartup(servers, STARTUP_TIMEOUT);
      assertRequestSucceeds(configPath, scenarioCommand("byzantine-leader", attackMode), VIEW_CHANGE_REQUEST_TIMEOUT, servers, "Cluster should recover after a Byzantine leader attack "
          + attackMode);
      if (attackMode == ByzantineAttackMode.PARTIAL_COMMIT_BROADCAST) {
        assertHonestReplicasRemainSafeWithoutImmediateConvergence(configPath, HONEST_WITH_BYZANTINE_LEADER_REPLICA_IDS);
      } else {
        assertHonestReplicasConverged(configPath, HONEST_WITH_BYZANTINE_LEADER_REPLICA_IDS);
      }
      assertByzantineAttackObserved(byzantineLeader, attackMode, "Byzantine leader attack was never exercised");
    } finally {
      try {
        stopProcesses(servers);
      } finally {
        cleanPersistedBlockData(configPath);
      }
    }
  }

  private static String scenarioCommand(String prefix, ByzantineAttackMode attackMode) {
    return prefix + "-" + attackMode.name().toLowerCase();
  }

  private static void assertHonestReplicasConverged(Path configPath, List<String> honestReplicaIds) throws Exception {
    ConfigParser config = ConfigParser.load(configPath);
    awaitFor("honest replica convergence").atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
      String expectedBlockHash = null;
      Long expectedHeight = null;
      for (String replicaId : honestReplicaIds) {
        BlockStore.BlockDocument latest = BlockStore.forReplica(config, replicaId).loadLatest().orElseThrow(() -> new AssertionError("Missing latest block for " + replicaId));
        if (expectedBlockHash == null) {
          expectedBlockHash = latest.blockHash();
          expectedHeight = latest.height();
          continue;
        }

        assertThat(latest.height()).as("honest replicas should agree on the latest persisted height after Byzantine scenario").isEqualTo(expectedHeight);
        assertThat(latest.blockHash()).as("honest replicas should agree on the latest persisted block after Byzantine scenario").isEqualTo(expectedBlockHash);
      }
    });
  }

  private static void assertHonestReplicasRemainSafeWithoutImmediateConvergence(Path configPath, List<String> honestReplicaIds) throws Exception {
    ConfigParser config = ConfigParser.load(configPath);
    awaitFor("honest replica safety without immediate convergence").atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
      Long maxHeight = null;
      String maxHeightHash = null;
      boolean observedProgress = false;
      for (String replicaId : honestReplicaIds) {
        BlockStore.BlockDocument latest = BlockStore.forReplica(config, replicaId).loadLatest().orElseThrow(() -> new AssertionError("Missing latest block for " + replicaId));
        if (latest.height() > 0L) {
          observedProgress = true;
        }
        if (maxHeight == null || latest.height() > maxHeight) {
          maxHeight = latest.height();
          maxHeightHash = latest.blockHash();
        } else if (latest.height() == maxHeight) {
          assertThat(latest.blockHash()).as("honest replicas that advanced furthest should agree on the latest persisted block").isEqualTo(maxHeightHash);
        }
      }
      assertThat(observedProgress).as("at least one honest replica should make persisted progress after the Byzantine leader attack").isTrue();
    });
  }
}
