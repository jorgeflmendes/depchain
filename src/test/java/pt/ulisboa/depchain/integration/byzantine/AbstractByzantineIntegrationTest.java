package pt.ulisboa.depchain.integration.byzantine;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import pt.ulisboa.depchain.integration.support.IntegrationHarness;
import pt.ulisboa.depchain.integration.support.IntegrationHarness.StartedServer;

abstract class AbstractByzantineIntegrationTest extends IntegrationHarness {
  protected final void runReplicaAttackScenario(ByzantineAttackMode attackMode) throws Exception {
    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    List<StartedServer> servers = new ArrayList<>(startServers(HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS, configPath));
    StartedServer byzantineReplica = startByzantineServer(BYZANTINE_REPLICA_ID, configPath, attackMode);
    servers.add(byzantineReplica);
    try {
      waitForServersStartup(servers, STARTUP_TIMEOUT);
      assertRequestSucceeds(configPath, scenarioCommand("byzantine-replica", attackMode), VIEW_CHANGE_REQUEST_TIMEOUT, servers, "Single Byzantine replica should not break cluster liveness for "
          + attackMode);
      assertByzantineAttackObserved(byzantineReplica, attackMode, "Byzantine replica attack was never exercised");
    } finally {
      stopProcesses(servers);
    }
  }

  protected final void runLeaderAttackScenario(ByzantineAttackMode attackMode) throws Exception {
    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    List<StartedServer> servers = new ArrayList<>(startServers(HONEST_WITH_BYZANTINE_LEADER_REPLICA_IDS, configPath));
    StartedServer byzantineLeader = startByzantineServer(LEADER_REPLICA_ID, configPath, attackMode);
    servers.add(byzantineLeader);
    try {
      waitForServersStartup(servers, STARTUP_TIMEOUT);
      assertRequestSucceeds(configPath, scenarioCommand("byzantine-leader", attackMode), VIEW_CHANGE_REQUEST_TIMEOUT, servers, "Cluster should recover after a Byzantine leader attack "
          + attackMode);
      assertByzantineAttackObserved(byzantineLeader, attackMode, "Byzantine leader attack was never exercised");
    } finally {
      stopProcesses(servers);
    }
  }

  private static String scenarioCommand(String prefix, ByzantineAttackMode attackMode) {
    return prefix + "-" + attackMode.name().toLowerCase();
  }
}
