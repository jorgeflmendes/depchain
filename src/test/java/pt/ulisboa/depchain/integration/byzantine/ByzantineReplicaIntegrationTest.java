package pt.ulisboa.depchain.integration.byzantine;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Tag("integration")
class ByzantineReplicaIntegrationTest extends AbstractByzantineIntegrationTest {
  @Test
  @Timeout(30)
  void dropsAllMessagesTest() throws Exception {
    runReplicaAttackScenario(ByzantineAttackMode.DROP_ALL_MESSAGES);
  }

  @Test
  @Timeout(30)
  void invalidNewViewTest() throws Exception {
    runReplicaAttackScenario(ByzantineAttackMode.INVALID_NEW_VIEW);
  }

  @Test
  @Timeout(30)
  void spoofedSenderIdTest() throws Exception {
    runReplicaAttackScenario(ByzantineAttackMode.SPOOFED_REPLICA_SENDER_ID);
  }

  @Test
  @Timeout(30)
  void invalidPrepareVoteTest() throws Exception {
    runReplicaAttackScenario(ByzantineAttackMode.INVALID_PREPARE_VOTE);
  }

  @Test
  @Timeout(30)
  void invalidPreCommitVoteTest() throws Exception {
    runReplicaAttackScenario(ByzantineAttackMode.INVALID_PRE_COMMIT_VOTE);
  }

  @Test
  @Timeout(30)
  void invalidCommitVoteTest() throws Exception {
    runReplicaAttackScenario(ByzantineAttackMode.INVALID_COMMIT_VOTE);
  }

  @Test
  @Timeout(30)
  void stalePrepareVoteTest() throws Exception {
    runReplicaAttackScenario(ByzantineAttackMode.STALE_PREPARE_VOTE);
  }

  @Test
  @Timeout(30)
  void stalePreCommitVoteTest() throws Exception {
    runReplicaAttackScenario(ByzantineAttackMode.STALE_PRE_COMMIT_VOTE);
  }

  @Test
  @Timeout(30)
  void staleCommitVoteTest() throws Exception {
    runReplicaAttackScenario(ByzantineAttackMode.STALE_COMMIT_VOTE);
  }
}
