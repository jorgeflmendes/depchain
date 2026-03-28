package pt.ulisboa.depchain.integration.byzantine;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import pt.ulisboa.depchain.integration.support.IntegrationHarness;
import pt.ulisboa.depchain.integration.support.IntegrationHarness.StartedServer;
import pt.ulisboa.depchain.server.node.BlockStore;
import pt.ulisboa.depchain.shared.config.ConfigParser;

abstract class AbstractByzantineIntegrationTest extends IntegrationHarness {
  protected final void runReplicaAttackScenario(ByzantineAttackMode attackMode) throws Exception {
    Path configPath = integrationConfigPath();
    cleanPersistedBlockData(configPath);
    populateConfig(configPath);

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
        try {
          cleanPersistedBlockData(configPath);
        } finally {
          cleanupIsolatedConfigDirectory(configPath);
        }
      }
    }
  }

  protected final void runLeaderAttackScenario(ByzantineAttackMode attackMode) throws Exception {
    Path configPath = integrationConfigPath();
    cleanPersistedBlockData(configPath);
    populateConfig(configPath);

    List<StartedServer> servers = new ArrayList<>(startServers(HONEST_WITH_BYZANTINE_LEADER_REPLICA_IDS, configPath));
    StartedServer byzantineLeader = startByzantineServer(LEADER_REPLICA_ID, configPath, attackMode);
    servers.add(byzantineLeader);
    try {
      waitForServersStartup(servers, STARTUP_TIMEOUT);
      assertRequestSucceeds(configPath, scenarioCommand("byzantine-leader", attackMode), VIEW_CHANGE_REQUEST_TIMEOUT, servers, "Cluster should recover after a Byzantine leader attack "
          + attackMode);
      assertHonestReplicasConverged(configPath, HONEST_WITH_BYZANTINE_LEADER_REPLICA_IDS);
      assertByzantineAttackObserved(byzantineLeader, attackMode, "Byzantine leader attack was never exercised");
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

  private static String scenarioCommand(String prefix, ByzantineAttackMode attackMode) {
    return prefix + "-" + attackMode.name().toLowerCase();
  }

  private static void assertHonestReplicasConverged(Path configPath, List<String> honestReplicaIds) throws Exception {
    ConfigParser config = ConfigParser.load(configPath);
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
  }
}
