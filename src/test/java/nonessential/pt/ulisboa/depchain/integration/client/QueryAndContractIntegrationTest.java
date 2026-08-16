package pt.ulisboa.depchain.integration.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;

import org.hyperledger.besu.datatypes.Address;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pt.ulisboa.depchain.client.api.ClientReplicaApi;
import pt.ulisboa.depchain.integration.support.IntegrationHarness;
import pt.ulisboa.depchain.integration.support.IntegrationHarness.ManagedCluster;
import pt.ulisboa.depchain.proto.QueryResponse;
import pt.ulisboa.depchain.proto.TransactionResponse;
import pt.ulisboa.depchain.server.execution.IstCoin;

@Tag("integration")
class QueryAndContractIntegrationTest extends IntegrationHarness {
  private static final String CLIENT_ID = "client";
  private static final String SPENDER_CLIENT_ID = "client2";
  private static final long INITIAL_ALLOWANCE = 60L;
  private static final long REAPPROVED_ALLOWANCE = 40L;

  @Test
  void approvalFrontRunningScenarioHoldsAtClusterLevel() throws Exception {
    try (ManagedCluster cluster = startManagedCluster(REPLICA_IDS);
        ClientReplicaApi owner = ClientReplicaApi.connect(cluster.configPath().toString(), CLIENT_ID);
        ClientReplicaApi spender = ClientReplicaApi.connect(cluster.configPath().toString(), SPENDER_CLIENT_ID)) {
      Address contractAddress = IstCoin.resolveContractAddress(cluster.configPath());
      String contractAddressText = contractAddress.toHexString().substring(2);
      Address ownerAddress = Address.fromHexString("0x" + owner.getWalletAddress());
      Address spenderAddress = Address.fromHexString("0x" + spender.getWalletAddress());

      TransactionResponse initialApproval = owner.callContract(contractAddressText, 0L, 0L, 300_000L, TEST_GAS_PRICE, IstCoin.encodeApproveCallData(spenderAddress, INITIAL_ALLOWANCE)
          .toArrayUnsafe());
      assertNotNull(initialApproval.getReceipt(), "Initial approval should return a receipt");
      assertTrue(initialApproval.getReceipt().getSuccess(), "Initial approval should succeed");

      TransactionResponse competingSpendResponse = spender.callContract(contractAddressText, 0L, 0L, 400_000L, 10L, IstCoin.encodeTransferFromCallData(ownerAddress, spenderAddress, INITIAL_ALLOWANCE)
          .toArrayUnsafe());
      TransactionResponse resetResponse = owner.callContract(contractAddressText, 0L, 1L, 300_000L, TEST_GAS_PRICE, IstCoin.encodeApproveCallData(spenderAddress, 0L)
          .toArrayUnsafe());
      assertNotNull(resetResponse.getReceipt(), "Reset request should return a receipt");
      assertNotNull(competingSpendResponse.getReceipt(), "Competing spend should return a receipt");
      assertTrue(resetResponse.getReceipt().getSuccess(), () -> "Zero-reset approval should succeed but failed with: " + resetResponse.getReceipt().getErrorMessage());
      assertTrue(competingSpendResponse.getReceipt()
          .getSuccess(), () -> "Front-running spend should succeed exactly once but failed with: " + competingSpendResponse.getReceipt().getErrorMessage());

      assertEquals(BigInteger.valueOf(INITIAL_ALLOWANCE), decodeUnsigned(spender.getIstCoinBalance(spender.getWalletAddress()).getReturnData().toByteArray()));

      TransactionResponse extraSpendAfterReset = spender.callContract(contractAddressText, 0L, 1L, 400_000L, 10L, IstCoin.encodeTransferFromCallData(ownerAddress, spenderAddress,
          REAPPROVED_ALLOWANCE).toArrayUnsafe());
      TransactionResponse explicitReapproval = owner.callContract(contractAddressText, 0L, 2L, 300_000L, TEST_GAS_PRICE, IstCoin.encodeApproveCallData(spenderAddress,
          REAPPROVED_ALLOWANCE).toArrayUnsafe());
      TransactionResponse approvedSpend = spender.callContract(contractAddressText, 0L, 2L, 400_000L, 10L, IstCoin.encodeTransferFromCallData(ownerAddress, spenderAddress,
          REAPPROVED_ALLOWANCE).toArrayUnsafe());
      TransactionResponse excessSpend = spender.callContract(contractAddressText, 0L, 3L, 400_000L, 10L, IstCoin.encodeTransferFromCallData(ownerAddress, spenderAddress, 1L)
          .toArrayUnsafe());

      assertNotNull(extraSpendAfterReset.getReceipt(), "Spend after reset should return a receipt");
      assertNotNull(explicitReapproval.getReceipt(), "Explicit reapproval should return a receipt");
      assertNotNull(approvedSpend.getReceipt(), "Approved spend should return a receipt");
      assertNotNull(excessSpend.getReceipt(), "Excess spend should return a receipt");
      assertFalse(extraSpendAfterReset.getReceipt().getSuccess(), "Spend after zero reset should fail until the owner explicitly reapproves");
      assertTrue(explicitReapproval.getReceipt()
          .getSuccess(), () -> "Reapproval after zero reset should succeed but failed with: " + explicitReapproval.getReceipt().getErrorMessage());
      assertTrue(approvedSpend.getReceipt().getSuccess(), () -> "Approved post-reset spend should succeed but failed with: " + approvedSpend.getReceipt().getErrorMessage());
      assertFalse(excessSpend.getReceipt().getSuccess(), "Additional spend without allowance should fail");

      QueryResponse spenderBalance = spender.getIstCoinBalance(spender.getWalletAddress());
      assertTrue(spenderBalance.getSuccess(), "Final spender balance query should succeed");
      assertEquals(BigInteger.valueOf(INITIAL_ALLOWANCE + REAPPROVED_ALLOWANCE), decodeUnsigned(spenderBalance.getReturnData()
          .toByteArray()), "Cluster-level frontrunning scenario should leave the spender with exactly the authorised amount");
    }
  }

  private static BigInteger decodeUnsigned(byte[] encoded) {
    return new BigInteger(1, encoded);
  }
}
