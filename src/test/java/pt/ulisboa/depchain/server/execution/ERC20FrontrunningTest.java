package pt.ulisboa.depchain.server.execution;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import pt.ulisboa.depchain.testsupport.EvmContractTestSupport;

@DisplayName("ERC-20 allowance frontrunning protection")
class ERC20FrontrunningTest extends EvmContractTestSupport {
  private static final Address OWNER = Address.fromHexString("0x1000000000000000000000000000000000000001");
  private static final Address SPENDER = Address.fromHexString("0x2000000000000000000000000000000000000002");

  @Nested
  @DisplayName("Approval transitions")
  class ApprovalTransitions {
    @Test
    @DisplayName("rejects direct non-zero to non-zero allowance changes")
    void directNonZeroToNonZeroAllowanceChangeIsRejected() {
      IstCoinContext context = createIstCoinContext(OWNER, SPENDER);

      EvmService.TransactionResult initialApproval = context.istCoin().approve(OWNER, SPENDER, 100L, 1L, CONTRACT_GAS_LIMIT, Wei.ONE);
      EvmService.TransactionResult reductionAttempt = context.istCoin().approve(OWNER, SPENDER, 50L, 2L, CONTRACT_GAS_LIMIT, Wei.ONE);

      assertSuccessfulBoolean(initialApproval);
      assertThat(reductionAttempt.success()).isFalse();
      assertThat(reductionAttempt.errorMessage()).isNotBlank();
      assertThat(decodeUnsigned(context.istCoin().getAllowance(OWNER, SPENDER).returnData())).isEqualTo(BigInteger.valueOf(100L));
    }
  }

  @Nested
  @DisplayName("Zero-reset mitigation")
  class ZeroResetMitigation {
    @Test
    @DisplayName("prevents double withdrawal across frontrun ordering")
    void zeroFirstApprovalFlowPreventsDoubleWithdrawalAcrossFrontRunOrdering() {
      IstCoinContext context = createIstCoinContext(OWNER, SPENDER);

      EvmService.TransactionResult initialApproval = context.istCoin().approve(OWNER, SPENDER, 100L, 1L, CONTRACT_GAS_LIMIT, Wei.ONE);

      EvmService.TransactionResult frontRunSpend = context.istCoin().transferFrom(SPENDER, OWNER, SPENDER, 100L, 0L, CONTRACT_GAS_LIMIT, Wei.of(10L));
      EvmService.TransactionResult zeroReset = context.istCoin().approve(OWNER, SPENDER, 0L, 2L, CONTRACT_GAS_LIMIT, Wei.ONE);
      EvmService.TransactionResult extraSpendAfterReset = context.istCoin().transferFrom(SPENDER, OWNER, SPENDER, 50L, 1L, CONTRACT_GAS_LIMIT, Wei.of(10L));
      EvmService.TransactionResult explicitReapproval = context.istCoin().approve(OWNER, SPENDER, 50L, 3L, CONTRACT_GAS_LIMIT, Wei.ONE);
      EvmService.TransactionResult approvedSpend = context.istCoin().transferFrom(SPENDER, OWNER, SPENDER, 50L, 2L, CONTRACT_GAS_LIMIT, Wei.of(10L));
      EvmService.TransactionResult excessSpend = context.istCoin().transferFrom(SPENDER, OWNER, SPENDER, 1L, 3L, CONTRACT_GAS_LIMIT, Wei.of(10L));

      assertSuccessfulBoolean(initialApproval);
      assertSuccessfulBoolean(frontRunSpend);
      assertSuccessfulBoolean(zeroReset);
      assertThat(extraSpendAfterReset.success()).isFalse();
      assertSuccessfulBoolean(explicitReapproval);
      assertSuccessfulBoolean(approvedSpend);
      assertThat(excessSpend.success()).isFalse();

      assertThat(decodeUnsigned(context.istCoin().getAllowance(OWNER, SPENDER).returnData())).isEqualTo(BigInteger.ZERO);
      assertThat(decodeUnsigned(context.istCoin().getBalance(OWNER).returnData())).isEqualTo(BigInteger.valueOf(9_999_999_850L));
      assertThat(decodeUnsigned(context.istCoin().getBalance(SPENDER).returnData())).isEqualTo(BigInteger.valueOf(150L));
    }
  }

  private static void assertSuccessfulBoolean(EvmService.TransactionResult result) {
    assertThat(result.errorMessage()).isNull();
    assertThat(result.success()).isTrue();
    assertThat(result.returnData().toArrayUnsafe()).containsExactly((byte) 1);
  }
}
