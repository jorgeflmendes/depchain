package pt.ulisboa.depchain.testsupport;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;

import pt.ulisboa.depchain.server.execution.EvmService;
import pt.ulisboa.depchain.server.execution.IstCoin;

public abstract class EvmContractTestSupport {
  protected static final long CONTRACT_GAS_LIMIT = 500_000L;
  protected static final Bytes IST_COIN_CREATION_BYTECODE = loadBytecode("ISTCoin.bin");

  protected static IstCoinContext createIstCoinContext(Address owner, Address... fundedAccounts) {
    EvmService evmService = new EvmService();
    evmService.createAccount(owner, 0L, Wei.of(50_000_000L));
    for (Address fundedAccount : fundedAccounts) {
      evmService.createAccount(fundedAccount, 0L, Wei.of(50_000_000L));
    }

    Address contractAddress = evmService.deployContract(owner, IST_COIN_CREATION_BYTECODE, abiEncodeAddress(owner));
    return new IstCoinContext(evmService, contractAddress, new IstCoin(evmService, contractAddress));
  }

  protected static BigInteger decodeUnsigned(Bytes encoded) {
    return new BigInteger(1, encoded.toArrayUnsafe());
  }

  protected static Bytes abiEncodeAddress(Address address) {
    byte[] padded = new byte[32];
    byte[] rawAddress = address.toArrayUnsafe();
    System.arraycopy(rawAddress, 0, padded, 32 - rawAddress.length, rawAddress.length);
    return Bytes.wrap(padded);
  }

  protected static Bytes loadBytecode(String fileName) {
    try (InputStream input = EvmContractTestSupport.class.getClassLoader().getResourceAsStream("contracts/" + fileName)) {
      if (input == null) {
        throw new IOException("Bytecode resource not found: contracts/" + fileName);
      }
      return Bytes.fromHexString("0x" + new String(input.readAllBytes(), StandardCharsets.UTF_8).trim());
    } catch (IOException exception) {
      throw new IllegalStateException("Could not load bytecode file " + fileName, exception);
    }
  }

  protected record IstCoinContext(EvmService evmService, Address contractAddress, IstCoin istCoin) {
  }
}
