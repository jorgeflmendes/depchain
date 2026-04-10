package pt.ulisboa.depchain.server.execution;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;

import pt.ulisboa.depchain.shared.config.GenesisParser;

public final class IstCoin {
  private static final int ABI_WORD_SIZE = 32;
  private static final Bytes BALANCE_OF_SELECTOR = Bytes.fromHexString("0x70a08231");
  private static final Bytes ALLOWANCE_SELECTOR = Bytes.fromHexString("0xdd62ed3e");
  private static final Bytes APPROVE_SELECTOR = Bytes.fromHexString("0x095ea7b3");
  private static final Bytes TRANSFER_SELECTOR = Bytes.fromHexString("0xa9059cbb");
  private static final Bytes TRANSFER_FROM_SELECTOR = Bytes.fromHexString("0x23b872dd");

  private final EvmService evmService;
  private final Address contractAddress;

  public IstCoin(EvmService evmService) throws IOException {
    this(evmService, resolveContractAddress());
  }

  public IstCoin(EvmService evmService, Address contractAddress) {
    this.evmService = pt.ulisboa.depchain.shared.validation.ValidationUtils.requireNonNull(evmService, "evmService");
    this.contractAddress = pt.ulisboa.depchain.shared.validation.ValidationUtils.requireNonNull(contractAddress, "contractAddress");
  }

  public EvmService.TransactionResult balanceOf(Address sender, Address owner, long nonce, long gasLimit, Wei gasPrice) {
    return callAndNormalize(sender, balanceOfCallData(owner), nonce, gasLimit, gasPrice, "invalid IST Coin balance response", IstCoin::normalizeUnsignedResult);
  }

  public EvmService.TransactionResult getBalance(Address owner) {
    try {
      Bytes returnData = evmService.callContract(contractAddress, contractAddress, balanceOfCallData(owner));
      return new EvmService.TransactionResult(true, 0L, normalizeUnsignedResult(returnData), null);
    } catch (RuntimeException exception) {
      return invalidResponse(0L, "invalid IST Coin balance response");
    }
  }

  public EvmService.TransactionResult allowance(Address sender, Address owner, Address spender, long nonce, long gasLimit, Wei gasPrice) {
    return callAndNormalize(sender, allowanceCallData(owner, spender), nonce, gasLimit, gasPrice, "invalid IST Coin allowance response", IstCoin::normalizeUnsignedResult);
  }

  public EvmService.TransactionResult getAllowance(Address owner, Address spender) {
    try {
      Bytes returnData = evmService.callContract(contractAddress, contractAddress, allowanceCallData(owner, spender));
      return new EvmService.TransactionResult(true, 0L, normalizeUnsignedResult(returnData), null);
    } catch (RuntimeException exception) {
      return invalidResponse(0L, "invalid IST Coin allowance response");
    }
  }

  public EvmService.TransactionResult approve(Address sender, Address spender, long amount, long nonce, long gasLimit, Wei gasPrice) {
    return callAndNormalize(sender, approveCallData(spender, amount), nonce, gasLimit, gasPrice, "invalid IST Coin approve response", IstCoin::normalizeBooleanResult);
  }

  public EvmService.TransactionResult transfer(Address sender, Address recipient, long amount, long nonce, long gasLimit, Wei gasPrice) {
    return callAndNormalize(sender, transferCallData(recipient, amount), nonce, gasLimit, gasPrice, "invalid IST Coin transfer response", IstCoin::normalizeBooleanResult);
  }

  public EvmService.TransactionResult transferFrom(Address sender, Address owner, Address recipient, long amount, long nonce, long gasLimit, Wei gasPrice) {
    return callAndNormalize(sender, transferFromCallData(owner, recipient, amount), nonce, gasLimit, gasPrice, "invalid IST Coin transferFrom response", IstCoin::normalizeBooleanResult);
  }

  public static Address resolveDefaultContractAddress() throws IOException {
    return resolveContractAddress(GenesisParser.loadDefault());
  }

  public static Address resolveContractAddress(java.nio.file.Path configPath) throws IOException {
    return resolveContractAddress(GenesisParser.loadForConfig(configPath));
  }

  public static Address resolveContractAddress(GenesisParser genesis) {
    pt.ulisboa.depchain.shared.validation.ValidationUtils.requireNonNull(genesis, "genesis");
    GenesisParser.GenesisTransaction deploy = genesis.transactions().stream().filter(GenesisParser.GenesisTransaction::isContractDeploy).findFirst()
        .orElseThrow(() -> new IllegalStateException("Genesis does not define an IST Coin contract deployment"));
    return Address.contractAddress(Address.fromHexString("0x" + deploy.from()), deploy.nonce());
  }

  public static Bytes encodeTransferCallData(Address recipient, long amount) {
    return transferCallData(recipient, amount);
  }

  public static Bytes encodeApproveCallData(Address spender, long amount) {
    return approveCallData(spender, amount);
  }

  public static Bytes encodeTransferFromCallData(Address owner, Address recipient, long amount) {
    return transferFromCallData(owner, recipient, amount);
  }

  private EvmService.TransactionResult callAndNormalize(Address sender, Bytes callData, long nonce, long gasLimit, Wei gasPrice, String invalidResponseMessage, Function<Bytes, Bytes> normalizer) {
    EvmService.TransactionResult execution = evmService.callContract(sender, contractAddress, callData, Wei.ZERO, nonce, gasLimit, gasPrice);
    if (!execution.success()) {
      return execution;
    }

    try {
      return new EvmService.TransactionResult(true, execution.gasUsed(), normalizer.apply(execution.returnData()), null);
    } catch (RuntimeException exception) {
      return invalidResponse(execution.gasUsed(), invalidResponseMessage);
    }
  }

  private static Address resolveContractAddress() throws IOException {
    return resolveDefaultContractAddress();
  }

  private static Bytes balanceOfCallData(Address owner) {
    return Bytes.concatenate(BALANCE_OF_SELECTOR, abiEncodeAddress(owner));
  }

  private static Bytes allowanceCallData(Address owner, Address spender) {
    return Bytes.concatenate(ALLOWANCE_SELECTOR, abiEncodeAddress(owner), abiEncodeAddress(spender));
  }

  private static Bytes approveCallData(Address spender, long amount) {
    if (amount < 0L) {
      throw new IllegalArgumentException("amount must be non-negative");
    }
    return Bytes.concatenate(APPROVE_SELECTOR, abiEncodeAddress(spender), abiEncodeUnsigned(BigInteger.valueOf(amount)));
  }

  private static Bytes transferCallData(Address recipient, long amount) {
    if (amount < 0L) {
      throw new IllegalArgumentException("amount must be non-negative");
    }
    return Bytes.concatenate(TRANSFER_SELECTOR, abiEncodeAddress(recipient), abiEncodeUnsigned(BigInteger.valueOf(amount)));
  }

  private static Bytes transferFromCallData(Address owner, Address recipient, long amount) {
    if (amount < 0L) {
      throw new IllegalArgumentException("amount must be non-negative");
    }
    return Bytes.concatenate(TRANSFER_FROM_SELECTOR, abiEncodeAddress(owner), abiEncodeAddress(recipient), abiEncodeUnsigned(BigInteger.valueOf(amount)));
  }

  private static Bytes abiEncodeAddress(Address address) {
    byte[] padded = new byte[ABI_WORD_SIZE];
    byte[] rawAddress = address.toArrayUnsafe();
    System.arraycopy(rawAddress, 0, padded, ABI_WORD_SIZE - rawAddress.length, rawAddress.length);
    return Bytes.wrap(padded);
  }

  private static Bytes abiEncodeUnsigned(BigInteger value) {
    if (value.signum() < 0) {
      throw new IllegalArgumentException("value must be non-negative");
    }

    byte[] raw = value.toByteArray();
    if (raw.length > ABI_WORD_SIZE && !(raw.length == ABI_WORD_SIZE + 1 && raw[0] == 0)) {
      throw new IllegalArgumentException("value exceeds 256 bits");
    }

    byte[] normalized = stripLeadingSignByte(raw);
    byte[] word = new byte[ABI_WORD_SIZE];
    System.arraycopy(normalized, 0, word, ABI_WORD_SIZE - normalized.length, normalized.length);
    return Bytes.wrap(word);
  }

  private static Bytes normalizeUnsignedResult(Bytes encoded) {
    if (encoded == null || encoded.isEmpty()) {
      throw new IllegalArgumentException("encoded result must not be empty");
    }

    byte[] normalized = stripLeadingSignByte(new BigInteger(1, encoded.toArrayUnsafe()).toByteArray());
    if (normalized.length == 0) {
      return Bytes.of(0);
    }
    return Bytes.wrap(normalized);
  }

  private static Bytes normalizeBooleanResult(Bytes encoded) {
    if (encoded == null || encoded.isEmpty()) {
      throw new IllegalArgumentException("encoded result must not be empty");
    }

    boolean value = !new BigInteger(1, encoded.toArrayUnsafe()).equals(BigInteger.ZERO);
    if (value) {
      return Bytes.of(1);
    }
    return Bytes.of(0);
  }

  private static byte[] stripLeadingSignByte(byte[] raw) {
    if (raw.length > 0 && raw[0] == 0) {
      return Arrays.copyOfRange(raw, 1, raw.length);
    }
    return raw;
  }

  private static EvmService.TransactionResult invalidResponse(long gasUsed, String errorMessage) {
    return new EvmService.TransactionResult(false, gasUsed, Bytes.EMPTY, errorMessage);
  }
}
