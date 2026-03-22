package pt.ulisboa.depchain.server.evm;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.math.BigInteger;
import java.util.Arrays;

import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.fluent.EVMExecutor;
import org.hyperledger.besu.evm.fluent.SimpleWorld;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class EvmService {
  public record TransactionResult(boolean success, long gasUsed, Bytes returnData, String errorMessage) {
  }

  private final EvmSpecVersion specVersion;
  private final SimpleWorld world;

  // TODO: replace placeholder gas defaults with the project gas schedule.
  private static final long DEFAULT_GAS_LIMIT = 10_000_000L;
  private static final long TRANSFER_GAS_USED = 21_000L;

  public EvmService() {
    this(EvmSpecVersion.CANCUN);
  }

  public EvmService(EvmSpecVersion specVersion) {
    this.specVersion = ValidationUtils.requireNonNull(specVersion, "specVersion");
    this.world = new SimpleWorld();
  }

  public MutableAccount createAccount(Address address, long nonce, Wei balance) {
    ValidationUtils.requireNonNull(address, "address");
    ValidationUtils.requireNonNegativeLong(nonce, "nonce");
    ValidationUtils.requireNonNull(balance, "balance");

    world.createAccount(address, nonce, balance);
    return account(address);
  }

  public MutableAccount account(Address address) {
    ValidationUtils.requireNonNull(address, "address");

    return (MutableAccount) world.get(address);
  }

  public SimpleWorld world() {
    return world;
  }

  public Address deployContract(Address sender, Bytes creationBytecode, Bytes constructorArguments) {
    ValidationUtils.requireAllNonNull(named("sender", sender), named("creationBytecode", creationBytecode), named("constructorArguments", constructorArguments));
    MutableAccount senderAccount = ValidationUtils.requirePresent(account(sender), "unknown sender account: " + sender);

    Address contractAddress = Address.contractAddress(sender, senderAccount.getNonce());
    Bytes initCode = Bytes.concatenate(creationBytecode, constructorArguments);

    Bytes runtimeBytecode = execute(sender, contractAddress, contractAddress, initCode, Bytes.EMPTY, Wei.ZERO, DEFAULT_GAS_LIMIT, Wei.ZERO, MessageFrame.Type.CONTRACT_CREATION)
        .returnData();
    MutableAccount contractAccount = ValidationUtils.requirePresent(account(contractAddress), "contract deployment did not create account: " + contractAddress);
    contractAccount.setCode(ValidationUtils.requireNonNull(runtimeBytecode, "runtimeBytecode"));
    senderAccount.incrementNonce();
    return contractAddress;
  }

  public Address deployContract(Address sender, Bytes creationBytecode) {
    return deployContract(sender, creationBytecode, Bytes.EMPTY);
  }

  public Bytes callContract(Address sender, Address contractAddress, Bytes callData) {
    ValidationUtils.requireNonNull(contractAddress, "contractAddress");
    MutableAccount contractAccount = ValidationUtils.requirePresent(account(contractAddress), "unknown contract account: " + contractAddress);

    return execute(sender, contractAddress, contractAddress, contractAccount.getCode(), ValidationUtils
        .requireNonNull(callData, "callData"), Wei.ZERO, DEFAULT_GAS_LIMIT, Wei.ZERO, MessageFrame.Type.MESSAGE_CALL).returnData();
  }

  public TransactionResult readNativeBalance(Address address) {
    ValidationUtils.requireNonNull(address, "address");

    MutableAccount account = account(address);
    Wei balance = Wei.ZERO;
    if (account != null) {
      balance = account.getBalance();
    }
    return new TransactionResult(true, 0L, normalizeUnsigned(balance.toBigInteger()), null);
  }

  public TransactionResult transferNative(Address sender, Address recipient, Wei amount, long nonce, long gasLimit, Wei gasPrice) {
    ValidationUtils.requireAllNonNull(named("sender", sender), named("recipient", recipient), named("amount", amount), named("gasPrice", gasPrice));
    ValidationUtils.requireNonNegativeLong(nonce, "nonce");
    ValidationUtils.requirePositiveLong(gasLimit, "gasLimit");

    MutableAccount senderAccount = ValidationUtils.requirePresent(account(sender), "unknown sender account: " + sender);
    if (senderAccount.getNonce() != nonce) {
      return new TransactionResult(false, 0L, Bytes.EMPTY, "invalid transaction nonce");
    }

    Wei maxFee = calculateFee(gasPrice, gasLimit, gasLimit);
    Wei requiredBalance = sumWei(amount, maxFee);
    if (senderAccount.getBalance().compareTo(requiredBalance) < 0) {
      return new TransactionResult(false, 0L, Bytes.EMPTY, "insufficient DepCoin balance for amount plus gas fee");
    }

    if (gasLimit < TRANSFER_GAS_USED) {
      Wei chargedFee = calculateFee(gasPrice, gasLimit, gasLimit);
      senderAccount.decrementBalance(chargedFee);
      senderAccount.incrementNonce();
      return new TransactionResult(false, gasLimit, Bytes.EMPTY, "insufficient gas for native transfer");
    }

    MutableAccount recipientAccount = account(recipient);
    if (recipientAccount == null) {
      world.createAccount(recipient, 0L, Wei.ZERO);
      recipientAccount = account(recipient);
    }

    senderAccount.decrementBalance(amount);
    recipientAccount.incrementBalance(amount);
    Wei chargedFee = calculateFee(gasPrice, gasLimit, TRANSFER_GAS_USED);
    senderAccount.decrementBalance(chargedFee);
    senderAccount.incrementNonce();
    return new TransactionResult(true, TRANSFER_GAS_USED, Bytes.EMPTY, null);
  }

  public TransactionResult callContract(Address sender, Address contractAddress, Bytes callData, Wei amount, long nonce, long gasLimit, Wei gasPrice) {
    ValidationUtils
        .requireAllNonNull(named("sender", sender), named("contractAddress", contractAddress), named("callData", callData), named("amount", amount), named("gasPrice", gasPrice));
    ValidationUtils.requireNonNegativeLong(nonce, "nonce");
    ValidationUtils.requirePositiveLong(gasLimit, "gasLimit");

    MutableAccount senderAccount = ValidationUtils.requirePresent(account(sender), "unknown sender account: " + sender);
    if (senderAccount.getNonce() != nonce) {
      return new TransactionResult(false, 0L, Bytes.EMPTY, "invalid transaction nonce");
    }

    MutableAccount contractAccount = account(contractAddress);
    if (contractAccount == null || contractAccount.getCode() == null || contractAccount.getCode().isEmpty()) {
      return new TransactionResult(false, 0L, Bytes.EMPTY, "unknown contract account");
    }

    Wei maxFee = calculateFee(gasPrice, gasLimit, gasLimit);
    Wei requiredBalance = sumWei(amount, maxFee);
    if (senderAccount.getBalance().compareTo(requiredBalance) < 0) {
      return new TransactionResult(false, 0L, Bytes.EMPTY, "insufficient DepCoin balance for amount plus gas fee");
    }

    TransactionResult execution = execute(sender, contractAddress, contractAddress, contractAccount
        .getCode(), callData, amount, gasLimit, gasPrice, MessageFrame.Type.MESSAGE_CALL);
    Wei chargedFee = calculateFee(gasPrice, gasLimit, execution.gasUsed());
    senderAccount.decrementBalance(chargedFee);
    senderAccount.incrementNonce();
    if (!execution.success()) {
      return new TransactionResult(false, execution.gasUsed(), execution.returnData(), execution.errorMessage());
    }
    return new TransactionResult(true, execution.gasUsed(), execution.returnData(), null);
  }

  private static Wei calculateFee(Wei gasPrice, long gasLimit, long gasUsed) {
    long boundedGasUsed = Math.max(gasUsed, 0L);
    long chargedGasUnits = Math.min(gasLimit, boundedGasUsed);
    return Wei.of(gasPrice.toBigInteger().multiply(BigInteger.valueOf(chargedGasUnits)));
  }

  private static Wei sumWei(Wei left, Wei right) {
    return Wei.of(left.toBigInteger().add(right.toBigInteger()));
  }

  private static Bytes normalizeUnsigned(BigInteger value) {
    byte[] raw = value.toByteArray();
    if (raw.length > 0 && raw[0] == 0) {
      raw = Arrays.copyOfRange(raw, 1, raw.length);
    }
    if (raw.length == 0) {
      return Bytes.of(0);
    }
    return Bytes.wrap(raw);
  }

  private TransactionResult execute(Address sender, Address receiver, Address contractAddress, Bytes code, Bytes callData, Wei value, long gasLimit, Wei gasPrice, MessageFrame.Type frameType) {
    ValidationUtils
        .requireAllNonNull(named("sender", sender), named("receiver", receiver), named("contractAddress", contractAddress), named("code", code), named("callData", callData), named("value", value), named("gasPrice", gasPrice), named("frameType", frameType));
    ValidationUtils.requirePositiveLong(gasLimit, "gasLimit");

    OutputCapturingTracer tracer = new OutputCapturingTracer();
    EVMExecutor executor = EVMExecutor.evm(specVersion);
    executor.sender(sender);
    executor.receiver(receiver);
    executor.contract(contractAddress);
    executor.code(code);
    executor.callData(callData);
    executor.ethValue(value);
    executor.gas(gasLimit);
    executor.gasLimit(gasLimit);
    executor.gasPriceGWei(gasPrice);
    executor.messageFrameType(frameType);
    executor.tracer(tracer);
    executor.worldUpdater(world.updater());
    executor.commitWorldState();
    Bytes executionResult = executor.execute();
    return tracer.snapshotOrFallback(executionResult, gasLimit);
  }

  private static final class OutputCapturingTracer implements OperationTracer {
    private Bytes topLevelOutput = Bytes.EMPTY;
    private Bytes topLevelReturn = Bytes.EMPTY;
    private Bytes revertReason = Bytes.EMPTY;
    private long remainingGas;
    private String exceptionalHaltDescription;

    @Override
    public void traceContextExit(MessageFrame frame) {
      if (frame.getDepth() == 0) {
        topLevelOutput = frame.getOutputData();
        topLevelReturn = frame.getReturnData();
        remainingGas = frame.getRemainingGas();
        if (frame.getRevertReason().isPresent()) {
          revertReason = frame.getRevertReason().get();
        }
        if (frame.getExceptionalHaltReason().isPresent()) {
          exceptionalHaltDescription = frame.getExceptionalHaltReason().get().getDescription();
        }
      }
    }

    private TransactionResult snapshotOrFallback(Bytes fallback, long gasLimit) {
      long gasUsed = gasLimit - remainingGas;
      if (gasUsed < 0L) {
        gasUsed = 0L;
      }
      if (gasUsed > gasLimit) {
        gasUsed = gasLimit;
      }

      Bytes result = fallback;
      if (topLevelOutput != null && !topLevelOutput.isEmpty()) {
        result = topLevelOutput;
      } else if (topLevelReturn != null && !topLevelReturn.isEmpty()) {
        result = topLevelReturn;
      }

      String errorMessage = null;
      boolean success = true;
      if (exceptionalHaltDescription != null && !exceptionalHaltDescription.isBlank()) {
        success = false;
        errorMessage = exceptionalHaltDescription;
      } else if (revertReason != null && !revertReason.isEmpty()) {
        success = false;
        errorMessage = "execution reverted: " + revertReason.toHexString();
      }

      return new TransactionResult(success, gasUsed, result, errorMessage);
    }
  }
}
