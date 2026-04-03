package pt.ulisboa.depchain.integration.cluster;

import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import pt.ulisboa.depchain.integration.support.ClusterIntegrationTestBase;
import pt.ulisboa.depchain.shared.network.links.fairloss.FairLossLink;

@Tag("integration")
@DisplayName("Packet loss tolerance")
class PacketLossClusterIntegrationTest extends ClusterIntegrationTestBase {
  private static final String DROP_PROBABILITY = "0.2";

  @Override
  protected Map<String, String> clusterJvmProperties() {
    return Map.of(FairLossLink.DROP_PROBABILITY_PROPERTY, DROP_PROBABILITY);
  }

  @Nested
  @DisplayName("Consensus progress")
  class ConsensusProgress {
    @ParameterizedTest(name = "request {0} reaches DECIDE")
    @ValueSource(ints = {1, 2, 3})
    void consensusStillReachesDecideUnderTwentyPercentPacketLoss(int requestIndex) throws Exception {
      cluster().assertRequestSucceeds("lossy-consensus-" + requestIndex, VIEW_CHANGE_REQUEST_TIMEOUT, "Client request should still reach DECIDE under packet loss");
    }
  }
}
