package pt.ulisboa.depchain.server.execution;

import static pt.ulisboa.depchain.shared.validation.ValidationUtils.named;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.fluent.EVMExecutor;
import org.hyperledger.besu.evm.fluent.SimpleWorld;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import pt.ulisboa.depchain.shared.model.AccountKind;
import pt.ulisboa.depchain.shared.validation.ValidationUtils;

public final class EvmService {
  public record TransactionResult(boolean success, long gasUsed, Bytes returnData, String errorMessage) {
  }

  private final EvmSpecVersion specVersion;
  private final SimpleWorld world;
  private final Map<Address, AccountKind> accountKinds;

  private static final long DEFAULT_GAS_LIMIT = 10_000_000L;
  private static final long TRANSFER_GAS_USED = 21_000L;

  public EvmService() {
    this(EvmSpecVersion.CANCUN);
  }

  public EvmService(EvmSpecVersion specVersion) {
    this.specVersion = ValidationUtils.requireNonNull(specVersion, "specVersion");
    this.world = new SimpleWorld();
    this.accountKinds = new HashMap<>();
  }

  public MutableAccount createAccount(Address address, long nonce, Wei balance) {
    return createAccount(address, nonce, balance, AccountKind.EOA);
  }

  public MutableAccount createAccount(Address address, long nonce, Wei balance, AccountKind accountKind) {
    ValidationUtils.requireNonNull(address, "address");
    ValidationUtils.requireNonNegativeLong(nonce, "nonce");
    ValidationUtils.requireNonNull(balance, "balance");
    ValidationUtils.requireNonNull(accountKind, "accountKind");

    world.createAccount(address, nonce, balance);
    MutableAccount createdAccount = account(address);
    accountKinds.put(address, accountKind);
    return createdAccount;
  }

  public MutableAccount account(Address address) {
    ValidationUtils.requireNonNull(address, "address");

    return (MutableAccount) world.get(address);
  }

  public SimpleWorld world() {
    return world;
  }

  public AccountKind accountKind(Address address) {
    ValidationUtils.requireNonNull(address, "address");
    return accountKinds.get(address);
  }

  public boolean isAccountKind(Address address, AccountKind expectedKind) {
    ValidationUtils.requireAllNonNull(named("address", address), named("expectedKind", expectedKind));
    return expectedKind.equals(accountKinds.get(address));
  }

  public void restoreAccountKind(Address address, AccountKind accountKind) {
    ValidationUtils.requireAllNonNull(named("address", address), named("accountKind", accountKind));
    ValidationUtils.requirePresent(account(address), "unknown account: " + address);
    accountKinds.put(address, accountKind);
  }

  public Address deployContract(Address sender, Bytes creationBytecode, Bytes constructorArguments) {
    ValidationUtils.requireAllNonNull(named("sender", sender), named("creationBytecode", creationBytecode), named("constructorArguments", constructorArguments));
    MutableAccount senderAccount = requireAccount(sender, AccountKind.EOA, "sender");

    Address contractAddress = Address.contractAddress(sender, senderAccount.getNonce());
    Bytes initCode = Bytes.concatenate(creationBytecode, constructorArguments);

    org.hyperledger.besu.evm.worldstate.WorldUpdater updater = world.updater();
    Bytes runtimeBytecode = execute(updater, sender, contractAddress, contractAddress, initCode, Bytes.EMPTY, Wei.ZERO, DEFAULT_GAS_LIMIT, Wei.ZERO, MessageFrame.Type.CONTRACT_CREATION)
        .returnData();
        
    MutableAccount updaterSenderAccount = updater.getAccount(sender);
    updaterSenderAccount.incrementNonce();
    
    MutableAccount contractAccount = ValidationUtils.requirePresent(updater.getAccount(contractAddress), "contract deployment did not create account: " + contractAddress);
    contractAccount.setCode(ValidationUtils.requireNonNull(runtimeBytecode, "runtimeBytecode"));
    accountKinds.put(contractAddress, AccountKind.CONTRACT);
    
    updater.commit();
    return contractAddress;
  }

  public Address deployContract(Address sender, Bytes creationBytecode) {
    return deployContract(sender, creationBytecode, Bytes.EMPTY);
  }

  public Bytes callContract(Address sender, Address contractAddress, Bytes callData) {
    ValidationUtils.requireNonNull(contractAddress, "contractAddress");
    MutableAccount contractAccount = requireAccount(contractAddress, AccountKind.CONTRACT, "contractAddress");

    org.hyperledger.besu.evm.worldstate.WorldUpdater updater = world.updater();
    pt.ulisboa.depchain.server.execution.EvmService.TransactionResult result = execute(updater, sender, contractAddress, contractAddress, contractAccount.getCode(), ValidationUtils
        .requireNonNull(callData, "callData"), Wei.ZERO, DEFAULT_GAS_LIMIT, Wei.ZERO, MessageFrame.Type.MESSAGE_CALL);
    updater.commit();
    return result.returnData();
  }

  public TransactionResult getNativeBalance(Address address) {
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

    MutableAccount senderAccount = requireAccount(sender, AccountKind.EOA, "sender");
    if (senderAccount.getNonce() != nonce) {
      return new TransactionResult(false, 0L, Bytes.EMPTY, "invalid transaction nonce");
    }

    AccountKind recipientKind = accountKind(recipient);
    if (recipientKind == AccountKind.CONTRACT) {
      return new TransactionResult(false, 0L, Bytes.EMPTY, "native transfer target is a contract account; use CONTRACT_CALL");
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

    if (account(recipient) == null) {
      createAccount(recipient, 0L, Wei.ZERO, AccountKind.EOA);
    }

    org.hyperledger.besu.evm.worldstate.WorldUpdater updater = world.updater();
    TransactionResult execution = execute(updater, sender, recipient, recipient, Bytes.EMPTY, Bytes.EMPTY, amount, gasLimit, gasPrice, MessageFrame.Type.MESSAGE_CALL);
    
    long effectiveGasUsed = Math.max(TRANSFER_GAS_USED, execution.gasUsed());
    Wei chargedFee = calculateFee(gasPrice, gasLimit, effectiveGasUsed);
    
    MutableAccount updaterSenderAccount = updater.getAccount(sender);
    updaterSenderAccount.decrementBalance(chargedFee);
    updaterSenderAccount.incrementNonce();
    
    updater.commit();
    if (!execution.success()) {
      return new TransactionResult(false, effectiveGasUsed, execution.returnData(), execution.errorMessage());
    }
    return new TransactionResult(true, effectiveGasUsed, Bytes.EMPTY, null);
  }

  public TransactionResult callContract(Address sender, Address contractAddress, Bytes callData, Wei amount, long nonce, long gasLimit, Wei gasPrice) {
    ValidationUtils
        .requireAllNonNull(named("sender", sender), named("contractAddress", contractAddress), named("callData", callData), named("amount", amount), named("gasPrice", gasPrice));
    ValidationUtils.requireNonNegativeLong(nonce, "nonce");
    ValidationUtils.requirePositiveLong(gasLimit, "gasLimit");

    MutableAccount senderAccount = requireAccount(sender, AccountKind.EOA, "sender");
    if (senderAccount.getNonce() != nonce) {
      return new TransactionResult(false, 0L, Bytes.EMPTY, "invalid transaction nonce");
    }

    AccountKind targetKind = accountKind(contractAddress);
    if (targetKind == null) {
      return new TransactionResult(false, 0L, Bytes.EMPTY, "unknown target account");
    }
    if (targetKind != AccountKind.CONTRACT) {
      return new TransactionResult(false, 0L, Bytes.EMPTY, "target account is not a contract");
    }

    MutableAccount contractAccount = account(contractAddress);
    if (contractAccount == null || contractAccount.getCode() == null || contractAccount.getCode().isEmpty()) {
      return new TransactionResult(false, 0L, Bytes.EMPTY, "contract account has no runtime bytecode");
    }

    Wei maxFee = calculateFee(gasPrice, gasLimit, gasLimit);
    Wei requiredBalance = sumWei(amount, maxFee);
    if (senderAccount.getBalance().compareTo(requiredBalance) < 0) {
      return new TransactionResult(false, 0L, Bytes.EMPTY, "insufficient DepCoin balance for amount plus gas fee");
    }

    org.hyperledger.besu.evm.worldstate.WorldUpdater updater = world.updater();
    TransactionResult execution = execute(updater, sender, contractAddress, contractAddress, contractAccount
        .getCode(), callData, amount, gasLimit, gasPrice, MessageFrame.Type.MESSAGE_CALL);
        
    Wei chargedFee = calculateFee(gasPrice, gasLimit, execution.gasUsed());
    
    MutableAccount updaterSenderAccount = updater.getAccount(sender);
    updaterSenderAccount.decrementBalance(chargedFee);
    updaterSenderAccount.incrementNonce();
    
    updater.commit();
    if (!execution.success()) {
      return new TransactionResult(false, execution.gasUsed(), execution.returnData(), execution.errorMessage());
    }
    return new TransactionResult(true, execution.gasUsed(), execution.returnData(), null);
  }

  private MutableAccount requireAccount(Address address, AccountKind expectedKind, String fieldName) {
    ValidationUtils.requireAllNonNull(named(fieldName, address), named("expectedKind", expectedKind));

    MutableAccount resolvedAccount = ValidationUtils.requirePresent(account(address), "unknown " + fieldName + " account: " + address);
    AccountKind actualKind = accountKinds.get(address);
    if (actualKind == null) {
      throw new IllegalStateException("unknown account kind for " + fieldName + " account: " + address);
    }
    if (actualKind != expectedKind) {
      throw new IllegalArgumentException(fieldName + " account must be " + expectedKind + " but was " + actualKind);
    }
    return resolvedAccount;
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

  private TransactionResult execute(org.hyperledger.besu.evm.worldstate.WorldUpdater updater, Address sender, Address receiver, Address contractAddress, Bytes code, Bytes callData, Wei value, long gasLimit, Wei gasPrice, MessageFrame.Type frameType) {
    ValidationUtils
        .requireAllNonNull(named("updater", updater), named("sender", sender), named("receiver", receiver), named("contractAddress", contractAddress), named("code", code), named("callData", callData), named("value", value), named("gasPrice", gasPrice), named("frameType", frameType));
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
    executor.worldUpdater(updater);
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
