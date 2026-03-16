package pt.ulisboa.depchain.integration.byzantine;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Tag("integration")
class ByzantineLeaderIntegrationTest extends AbstractByzantineIntegrationTest {
  @Test
  @Timeout(35)
  void invalidPrepareProposalQcTest() throws Exception {
    runLeaderAttackScenario(ByzantineAttackMode.INVALID_PREPARE_PROPOSAL_QC);
  }

  @Test
  @Timeout(35)
  void invalidPreCommitQcTest() throws Exception {
    runLeaderAttackScenario(ByzantineAttackMode.INVALID_PRE_COMMIT_QC);
  }

  @Test
  @Timeout(35)
  void invalidCommitQcTest() throws Exception {
    runLeaderAttackScenario(ByzantineAttackMode.INVALID_COMMIT_QC);
  }

  @Test
  @Timeout(35)
  void invalidDecideQcTest() throws Exception {
    runLeaderAttackScenario(ByzantineAttackMode.INVALID_DECIDE_QC);
  }
}
