package pt.ulisboa.depchain.server.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.junit.jupiter.api.Test;

class ERC20FrontrunningTest {
  private static final Address OWNER = Address.fromHexString("0x1000000000000000000000000000000000000001");
  private static final Address SPENDER = Address.fromHexString("0x2000000000000000000000000000000000000002");
  private static final Bytes IST_COIN_CREATION_BYTECODE = loadBytecode("ISTCoin.bin");
  private static final long GAS_LIMIT = 500_000L;

  @Test
  void directNonZeroToNonZeroAllowanceChangeIsRejected() {
    TestContext context = createContext();

    EvmService.TransactionResult initialApproval = context.istCoin.approve(OWNER, SPENDER, 100L, 1L, GAS_LIMIT, Wei.ONE);
    EvmService.TransactionResult reductionAttempt = context.istCoin.approve(OWNER, SPENDER, 50L, 2L, GAS_LIMIT, Wei.ONE);

    assertSuccessfulBoolean(initialApproval);
    assertFalse(reductionAttempt.success());
    assertNotNull(reductionAttempt.errorMessage());
    assertEquals(BigInteger.valueOf(100L), decodeUnsigned(context.istCoin.getAllowance(OWNER, SPENDER).returnData()));
  }

  @Test
  void zeroFirstApprovalFlowPreventsDoubleWithdrawalAcrossFrontRunOrdering() {
    TestContext context = createContext();

    EvmService.TransactionResult initialApproval = context.istCoin.approve(OWNER, SPENDER, 100L, 1L, GAS_LIMIT, Wei.ONE);

    // Emulates the malicious spender winning ordering with a higher gas price.
    EvmService.TransactionResult frontRunSpend = context.istCoin.transferFrom(SPENDER, OWNER, SPENDER, 100L, 0L, GAS_LIMIT, Wei.of(10L));
    EvmService.TransactionResult zeroReset = context.istCoin.approve(OWNER, SPENDER, 0L, 2L, GAS_LIMIT, Wei.ONE);
    EvmService.TransactionResult extraSpendAfterReset = context.istCoin.transferFrom(SPENDER, OWNER, SPENDER, 50L, 1L, GAS_LIMIT, Wei.of(10L));
    EvmService.TransactionResult explicitReapproval = context.istCoin.approve(OWNER, SPENDER, 50L, 3L, GAS_LIMIT, Wei.ONE);
    EvmService.TransactionResult approvedSpend = context.istCoin.transferFrom(SPENDER, OWNER, SPENDER, 50L, 2L, GAS_LIMIT, Wei.of(10L));
    EvmService.TransactionResult excessSpend = context.istCoin.transferFrom(SPENDER, OWNER, SPENDER, 1L, 3L, GAS_LIMIT, Wei.of(10L));

    assertSuccessfulBoolean(initialApproval);
    assertSuccessfulBoolean(frontRunSpend);
    assertSuccessfulBoolean(zeroReset);
    assertFalse(extraSpendAfterReset.success());
    assertSuccessfulBoolean(explicitReapproval);
    assertSuccessfulBoolean(approvedSpend);
    assertFalse(excessSpend.success());

    assertEquals(BigInteger.ZERO, decodeUnsigned(context.istCoin.getAllowance(OWNER, SPENDER).returnData()));
    assertEquals(BigInteger.valueOf(9_999_999_850L), decodeUnsigned(context.istCoin.getBalance(OWNER).returnData()));
    assertEquals(BigInteger.valueOf(150L), decodeUnsigned(context.istCoin.getBalance(SPENDER).returnData()));
  }

  private static TestContext createContext() {
    EvmService evmService = new EvmService();
    evmService.createAccount(OWNER, 0L, Wei.of(50_000_000L));
    evmService.createAccount(SPENDER, 0L, Wei.of(50_000_000L));
    Address contractAddress = evmService.deployContract(OWNER, IST_COIN_CREATION_BYTECODE, abiEncodeAddress(OWNER));
    return new TestContext(new IstCoin(evmService, contractAddress));
  }

  private static void assertSuccessfulBoolean(EvmService.TransactionResult result) {
    assertTrue(result.success(), result.errorMessage());
    assertEquals(Bytes.of(1), result.returnData());
  }

  private static BigInteger decodeUnsigned(Bytes encoded) {
    return new BigInteger(1, encoded.toArrayUnsafe());
  }

  private static Bytes abiEncodeAddress(Address address) {
    byte[] padded = new byte[32];
    byte[] rawAddress = address.toArrayUnsafe();
    System.arraycopy(rawAddress, 0, padded, 32 - rawAddress.length, rawAddress.length);
    return Bytes.wrap(padded);
  }

  private static Bytes loadBytecode(String fileName) {
    try (InputStream input = ERC20FrontrunningTest.class.getClassLoader().getResourceAsStream("contracts/" + fileName)) {
      if (input == null) {
        throw new IOException("Bytecode resource not found: contracts/" + fileName);
      }
      return Bytes.fromHexString("0x" + new String(input.readAllBytes(), StandardCharsets.UTF_8).trim());
    } catch (IOException exception) {
      throw new IllegalStateException("Could not load bytecode file " + fileName, exception);
    }
  }

  private record TestContext(IstCoin istCoin) {
  }
}
