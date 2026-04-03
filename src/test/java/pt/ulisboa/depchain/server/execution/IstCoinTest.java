package pt.ulisboa.depchain.server.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.junit.jupiter.api.Test;

class IstCoinTest {
  private static final Address SENDER = Address.fromHexString("0x1000000000000000000000000000000000000001");
  private static final Address RECIPIENT = Address.fromHexString("0x2000000000000000000000000000000000000002");
  private static final Bytes IST_COIN_CREATION_BYTECODE = loadBytecode("ISTCoin.bin");

  @Test
  void balanceOfReturnsNormalizedUnsignedValue() {
    EvmService evmService = new EvmService();
    evmService.createAccount(SENDER, 0L, Wei.of(2_000_000));
    Address contractAddress = evmService.deployContract(SENDER, IST_COIN_CREATION_BYTECODE, abiEncodeAddress(SENDER));
    IstCoin istCoin = new IstCoin(evmService, contractAddress);

    EvmService.TransactionResult result = istCoin.balanceOf(SENDER, SENDER, 1L, 500_000L, Wei.ONE);

    assertTrue(result.success());
    assertEquals(BigInteger.valueOf(10_000_000_000L), new BigInteger(1, result.returnData().toArrayUnsafe()));
  }

  @Test
  void transferReturnsNormalizedBooleanValue() {
    EvmService evmService = new EvmService();
    evmService.createAccount(SENDER, 0L, Wei.of(2_000_000));
    Address contractAddress = evmService.deployContract(SENDER, IST_COIN_CREATION_BYTECODE, abiEncodeAddress(SENDER));
    IstCoin istCoin = new IstCoin(evmService, contractAddress);

    EvmService.TransactionResult transfer = istCoin.transfer(SENDER, RECIPIENT, 25L, 1L, 500_000L, Wei.ONE);
    EvmService.TransactionResult recipientBalance = istCoin.balanceOf(SENDER, RECIPIENT, 2L, 500_000L, Wei.ONE);

    assertTrue(transfer.success());
    assertEquals(Bytes.of(1), transfer.returnData());
    assertTrue(recipientBalance.success());
    assertEquals(BigInteger.valueOf(25L), new BigInteger(1, recipientBalance.returnData().toArrayUnsafe()));
  }

  private static Bytes abiEncodeAddress(Address address) {
    byte[] padded = new byte[32];
    byte[] rawAddress = address.toArrayUnsafe();
    System.arraycopy(rawAddress, 0, padded, 32 - rawAddress.length, rawAddress.length);
    return Bytes.wrap(padded);
  }

  private static Bytes loadBytecode(String fileName) {
    try (InputStream input = IstCoinTest.class.getClassLoader().getResourceAsStream("contracts/" + fileName)) {
      if (input == null) {
        throw new IOException("Bytecode resource not found: contracts/" + fileName);
      }
      return Bytes.fromHexString("0x" + new String(input.readAllBytes(), StandardCharsets.UTF_8).trim());
    } catch (IOException exception) {
      throw new IllegalStateException("Could not load bytecode file " + fileName, exception);
    }
  }
}
