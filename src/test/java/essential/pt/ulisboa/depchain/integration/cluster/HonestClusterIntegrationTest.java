package pt.ulisboa.depchain.integration.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pt.ulisboa.depchain.integration.support.ClusterIntegrationTestBase;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;

@Tag("integration")
@DisplayName("Honest cluster behaviour")
class HonestClusterIntegrationTest extends ClusterIntegrationTestBase {
  @Nested
  @DisplayName("Happy path")
  class HappyPath {
    @Test
    @DisplayName("replayed client requests are ignored")
    void replayedClientRequestTest() throws Exception {
      cluster().assertReplayIsIgnored("replayed-test", "Replayed client request should not receive a response");
    }

    @Test
    @DisplayName("forged client signatures are rejected")
    void forgedClientSignatureTest() throws Exception {
      InboundPacket response = cluster().sendForgedClientRequest(LEADER_REPLICA_ID, "forged-test");
      assertThat(response).isNull();
    }
  }
}
