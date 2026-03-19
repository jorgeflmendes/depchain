package pt.ulisboa.depchain.server.evm;

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
  private final EvmSpecVersion specVersion;
  private final SimpleWorld world;

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
    ValidationUtils.requireNonNull(sender, "sender");
    MutableAccount senderAccount = ValidationUtils.requirePresent(account(sender), "unknown sender account: " + sender);

    Address contractAddress = Address.contractAddress(sender, senderAccount.getNonce());
    Bytes initCode = Bytes
        .concatenate(ValidationUtils.requireNonNull(creationBytecode, "creationBytecode"), ValidationUtils.requireNonNull(constructorArguments, "constructorArguments"));

    Bytes runtimeBytecode = execute(sender, contractAddress, contractAddress, initCode, Bytes.EMPTY, MessageFrame.Type.CONTRACT_CREATION);
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

    return execute(sender, contractAddress, contractAddress, contractAccount.getCode(), ValidationUtils.requireNonNull(callData, "callData"), MessageFrame.Type.MESSAGE_CALL);
  }

  private Bytes execute(Address sender, Address receiver, Address contractAddress, Bytes code, Bytes callData, MessageFrame.Type frameType) {
    ValidationUtils.requireNonNull(sender, "sender");
    ValidationUtils.requireNonNull(receiver, "receiver");
    ValidationUtils.requireNonNull(contractAddress, "contractAddress");
    ValidationUtils.requireNonNull(code, "code");
    ValidationUtils.requireNonNull(callData, "callData");
    ValidationUtils.requireNonNull(frameType, "frameType");

    OutputCapturingTracer tracer = new OutputCapturingTracer();
    EVMExecutor executor = EVMExecutor.evm(specVersion);
    executor.sender(sender);
    executor.receiver(receiver);
    executor.contract(contractAddress);
    executor.code(code);
    executor.callData(callData);
    executor.messageFrameType(frameType);
    executor.tracer(tracer);
    executor.worldUpdater(world.updater());
    executor.commitWorldState();
    Bytes executionResult = executor.execute();
    return tracer.resultOrFallback(executionResult);
  }

  private static final class OutputCapturingTracer implements OperationTracer {
    private Bytes topLevelOutput = Bytes.EMPTY;
    private Bytes topLevelReturn = Bytes.EMPTY;

    @Override
    public void traceContextExit(MessageFrame frame) {
      if (frame.getDepth() == 0) {
        topLevelOutput = frame.getOutputData();
        topLevelReturn = frame.getReturnData();
      }
    }

    private Bytes resultOrFallback(Bytes fallback) {
      if (topLevelOutput != null && !topLevelOutput.isEmpty()) {
        return topLevelOutput;
      }
      if (topLevelReturn != null && !topLevelReturn.isEmpty()) {
        return topLevelReturn;
      }
      return fallback;
    }
  }
}
