package pt.ulisboa.depchain.server.evm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.junit.jupiter.api.Test;

class EvmServiceTest {
  private static final Address SENDER = Address.fromHexString("0x1000000000000000000000000000000000000001");
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
    try {
      Path bytecodePath = Path.of(System.getProperty("user.dir"), "src", "test", "resources", "contracts", fileName);
      return Bytes.fromHexString("0x" + Files.readString(bytecodePath).trim());
    } catch (IOException exception) {
      throw new IllegalStateException("Could not load bytecode file " + fileName, exception);
    }
  }
}
