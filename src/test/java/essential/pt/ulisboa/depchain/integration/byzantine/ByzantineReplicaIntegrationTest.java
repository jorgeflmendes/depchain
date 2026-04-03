package pt.ulisboa.depchain.integration.byzantine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Tag("integration")
@DisplayName("Byzantine replica tolerance")
class ByzantineReplicaIntegrationTest extends AbstractByzantineIntegrationTest {
  @ParameterizedTest(name = "{0} does not break liveness")
  @EnumSource(value = ByzantineAttackMode.class, names = {"DROP_ALL_MESSAGES", "INVALID_NEW_VIEW", "SPOOFED_REPLICA_SENDER_ID", "INVALID_PREPARE_VOTE", "INVALID_PRE_COMMIT_VOTE",
      "INVALID_COMMIT_VOTE", "STALE_PREPARE_VOTE", "STALE_PRE_COMMIT_VOTE", "STALE_COMMIT_VOTE"})
  void replicaAttackDoesNotBreakClusterLiveness(ByzantineAttackMode attackMode) throws Exception {
    runReplicaAttackScenario(attackMode);
  }
}
