package pt.ulisboa.depchain.integration.cluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.math.BigInteger;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import pt.ulisboa.depchain.client.api.ClientReplicaApi;
import pt.ulisboa.depchain.integration.support.ClusterIntegrationTestBase;
import pt.ulisboa.depchain.shared.network.links.fairloss.FairLossLink;

@Tag("integration")
@DisplayName("Network fault injection")
class AdversarialNetworkClusterIntegrationTest extends ClusterIntegrationTestBase {
  private static final String DUPLICATE_PROBABILITY = "0.35";
  private static final String ASYNC_MAX_DELAY_MS = "25";
  private static final String EXACTLY_ONCE_CLIENT_ID = "client2";
  private static final String EXACTLY_ONCE_RECIPIENT = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

  @Override
  protected Map<String, String> clusterJvmProperties() {
    return Map.of(FairLossLink.DUPLICATE_PROBABILITY_PROPERTY, DUPLICATE_PROBABILITY, FairLossLink.ASYNC_MAX_DELAY_MS_PROPERTY, ASYNC_MAX_DELAY_MS);
  }

  @Nested
  @DisplayName("Liveness")
  class Liveness {
    @ParameterizedTest(name = "request {0} reaches DECIDE")
    @ValueSource(ints = {1, 2, 3})
    void consensusRemainsLiveUnderDuplicatedAndReorderedPackets(int requestIndex) throws Exception {
      cluster().assertRequestSucceeds("duplicate-reorder-consensus-"
          + requestIndex, VIEW_CHANGE_REQUEST_TIMEOUT, "Client request should still reach consensus under duplication and reordering");
    }
  }

  @Nested
  @DisplayName("Exactly-once effects")
  class ExactlyOnceEffects {
    @Test
    @DisplayName("duplicated transport traffic does not cause double execution")
    void duplicatedTransportTrafficDoesNotCauseDoubleExecution() throws Exception {
      try (ClientReplicaApi client = ClientReplicaApi.connect(cluster().configPath().toString(), EXACTLY_ONCE_CLIENT_ID)) {
        assertThat(client.transferDepCoin(EXACTLY_ONCE_RECIPIENT, 1L, 0L, TEST_GAS_LIMIT, TEST_GAS_PRICE).getReceipt().getSuccess()).isTrue();

        await().forever().untilAsserted(() -> {
          BigInteger recipientBalance = new BigInteger(1, client.getDepCoinBalance(EXACTLY_ONCE_RECIPIENT).getReturnData().toByteArray());
          assertThat(recipientBalance).as("duplicated transport traffic must not execute the same logical request twice").isEqualTo(BigInteger.ONE);
        });
      }
    }
  }
}
