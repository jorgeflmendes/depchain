package pt.ulisboa.depchain.server.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.junit.jupiter.api.Test;

class EvmServiceTest {
  private static final Address SENDER = Address.fromHexString("0x1000000000000000000000000000000000000001");
  private static final Address RECIPIENT = Address.fromHexString("0x2000000000000000000000000000000000000002");
  private static final Bytes NAME_SELECTOR = Bytes.fromHexString("0x06fdde03");
  private static final Bytes SYMBOL_SELECTOR = Bytes.fromHexString("0x95d89b41");
  private static final Bytes DECIMALS_SELECTOR = Bytes.fromHexString("0x313ce567");
  private static final Bytes TOTAL_SUPPLY_SELECTOR = Bytes.fromHexString("0x18160ddd");
  private static final Bytes IST_COIN_CREATION_BYTECODE = loadIstCoinCreationBytecode();

  @Test
  void createAccountStoresAddressNonceAndBalance() {
    EvmService evmService = new EvmService();

    MutableAccount account = evmService.createAccount(SENDER, 7L, Wei.of(25));

    assertNotNull(account);
    assertEquals(SENDER, account.getAddress());
    assertEquals(7L, account.getNonce());
    assertEquals(Wei.of(25), account.getBalance());
  }

  @Test
  void deployContractRunsConstructorAndExposesIstCoinMetadata() {
    EvmService evmService = new EvmService();
    MutableAccount senderAccount = evmService.createAccount(SENDER, 0L, Wei.of(100));

    Address contractAddress = evmService.deployContract(SENDER, IST_COIN_CREATION_BYTECODE, abiEncodeAddress(SENDER));
    MutableAccount deployedContract = evmService.account(contractAddress);

    assertNotNull(deployedContract);
    assertNotNull(deployedContract.getCode());
    assertEquals(1L, senderAccount.getNonce());
    assertEquals("IST Coin", decodeAbiString(evmService.callContract(SENDER, contractAddress, NAME_SELECTOR)));
    assertEquals("IST", decodeAbiString(evmService.callContract(SENDER, contractAddress, SYMBOL_SELECTOR)));
    assertEquals(2, decodeAbiUInt(evmService.callContract(SENDER, contractAddress, DECIMALS_SELECTOR)).intValueExact());
    assertEquals(BigInteger.valueOf(10_000_000_000L), decodeAbiUInt(evmService.callContract(SENDER, contractAddress, TOTAL_SUPPLY_SELECTOR)));
    assertEquals(BigInteger.valueOf(10_000_000_000L), decodeAbiUInt(evmService.callContract(SENDER, contractAddress, balanceOfCallData(SENDER))));
  }

  @Test
  void nativeTransferConsumesGasMovesBalanceAndIncrementsNonce() {
    EvmService evmService = new EvmService();
    MutableAccount senderAccount = evmService.createAccount(SENDER, 0L, Wei.of(50_000));

    EvmService.TransactionResult result = evmService.transferNative(SENDER, RECIPIENT, Wei.of(100), 0L, 21_000L, Wei.ONE);

    assertTrue(result.success());
    assertEquals(21_000L, result.gasUsed());
    assertEquals(Bytes.EMPTY, result.returnData());
    assertNull(result.errorMessage());
    assertEquals(1L, senderAccount.getNonce());
    assertEquals(Wei.of(28_900), senderAccount.getBalance());
    assertEquals(Wei.of(100), evmService.account(RECIPIENT).getBalance());
  }

  @Test
  void transferWithInsufficientGasLimitFailsAndChargesUsedGasFee() {
    EvmService evmService = new EvmService();
    MutableAccount senderAccount = evmService.createAccount(SENDER, 0L, Wei.of(50_000));

    EvmService.TransactionResult result = evmService.transferNative(SENDER, RECIPIENT, Wei.of(100), 0L, 10_000L, Wei.ONE);

    assertFalse(result.success());
    assertEquals(10_000L, result.gasUsed());
    assertEquals("insufficient gas for native transfer", result.errorMessage());
    assertEquals(1L, senderAccount.getNonce());
    assertEquals(Wei.of(40_000), senderAccount.getBalance());
    assertNull(evmService.account(RECIPIENT));
  }

  @Test
  void contractCallConsumesGasAndReturnsData() {
    EvmService evmService = new EvmService();
    MutableAccount senderAccount = evmService.createAccount(SENDER, 0L, Wei.of(2_000_000));
    Address contractAddress = evmService.deployContract(SENDER, IST_COIN_CREATION_BYTECODE, abiEncodeAddress(SENDER));

    EvmService.TransactionResult result = evmService.callContract(SENDER, contractAddress, NAME_SELECTOR, Wei.ZERO, 1L, 500_000L, Wei.ONE);

    assertTrue(result.success());
    assertTrue(result.gasUsed() > 0L);
    assertEquals("IST Coin", decodeAbiString(result.returnData()));
    assertNull(result.errorMessage());
    assertEquals(2L, senderAccount.getNonce());
    assertEquals(Wei.of(2_000_000L - result.gasUsed()), senderAccount.getBalance());
  }

  private static Bytes balanceOfCallData(Address address) {
    return Bytes.concatenate(Bytes.fromHexString("0x70a08231"), abiEncodeAddress(address));
  }

  private static Bytes abiEncodeAddress(Address address) {
    byte[] padded = new byte[32];
    byte[] rawAddress = address.toArrayUnsafe();
    System.arraycopy(rawAddress, 0, padded, 32 - rawAddress.length, rawAddress.length);
    return Bytes.wrap(padded);
  }

  private static String decodeAbiString(Bytes encoded) {
    int offset = decodeAbiUInt(encoded.slice(0, 32)).intValueExact();
    int length = decodeAbiUInt(encoded.slice(offset, 32)).intValueExact();
    return new String(encoded.slice(offset + 32, length).toArrayUnsafe(), StandardCharsets.UTF_8);
  }

  private static BigInteger decodeAbiUInt(Bytes encoded) {
    return new BigInteger(1, encoded.toArrayUnsafe());
  }

  private static Bytes loadIstCoinCreationBytecode() {
    return loadBytecode("ISTCoin.bin");
  }

  private static Bytes loadBytecode(String fileName) {
    try (InputStream input = EvmServiceTest.class.getClassLoader().getResourceAsStream("contracts/" + fileName)) {
      if (input == null) {
        throw new IOException("Bytecode resource not found: contracts/" + fileName);
      }
      return Bytes.fromHexString("0x" + new String(input.readAllBytes(), StandardCharsets.UTF_8).trim());
    } catch (IOException exception) {
      throw new IllegalStateException("Could not load bytecode file " + fileName, exception);
    }
  }
}
