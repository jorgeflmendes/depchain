package pt.ulisboa.depchain.integration.cluster;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import pt.ulisboa.depchain.integration.support.IntegrationHarness;
import pt.ulisboa.depchain.integration.support.IntegrationHarness.ManagedCluster;
import pt.ulisboa.depchain.shared.network.links.fairloss.FairLossLink;

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PacketLossClusterIntegrationTest extends IntegrationHarness {
  private static final String DROP_PROBABILITY = "0.2";

  private ManagedCluster sharedCluster;
  private String previousDropProbability;

  @BeforeAll
  void startSharedCluster() throws Exception {
    previousDropProbability = System.getProperty(FairLossLink.DROP_PROBABILITY_PROPERTY);
    System.setProperty(FairLossLink.DROP_PROBABILITY_PROPERTY, DROP_PROBABILITY);
    sharedCluster = startManagedCluster(REPLICA_IDS, java.util.Map.of(FairLossLink.DROP_PROBABILITY_PROPERTY, DROP_PROBABILITY));
  }

  @AfterAll
  void stopSharedCluster() throws Exception {
    try {
      if (sharedCluster != null) {
        sharedCluster.close();
      }
    } finally {
      if (previousDropProbability == null) {
        System.clearProperty(FairLossLink.DROP_PROBABILITY_PROPERTY);
      } else {
        System.setProperty(FairLossLink.DROP_PROBABILITY_PROPERTY, previousDropProbability);
      }
    }
  }

  @Test
  @Timeout(90)
  void consensusStillReachesDecideUnderTwentyPercentPacketLoss() throws Exception {
    for (int i = 1; i <= 3; i++) {
      sharedCluster.assertRequestSucceeds("lossy-consensus-" + i, VIEW_CHANGE_REQUEST_TIMEOUT, "Client request should still reach DECIDE under packet loss");
    }
  }
}
