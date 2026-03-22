package pt.ulisboa.depchain.client.shell;

import java.io.PrintWriter;
import java.math.BigInteger;
import java.util.HexFormat;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.ulisboa.depchain.client.DpchClient;
import pt.ulisboa.depchain.client.IncoherentReplicaResponseException;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Parameters;

final class ClientShellCommandSet {
  private static final Logger logger = LoggerFactory.getLogger(ClientShellCommandSet.class);
  private static final long DEFAULT_DEP_TRANSFER_GAS_LIMIT = 21_000L;
  private static final long DEFAULT_CONTRACT_CALL_GAS_LIMIT = 250_000L;
  private static final long DEFAULT_GAS_PRICE = 1L;

  private final DpchClient client;
  private final PrintWriter out;
  private final PrintWriter err;
  private final AtomicBoolean exitRequested;

  ClientShellCommandSet(DpchClient client, PrintWriter out, PrintWriter err) {
    this.client = client;
    this.out = out;
    this.err = err;
    this.exitRequested = new AtomicBoolean();
  }

  CommandLine commandLine() {
    CommandLine commandLine = new CommandLine(new RootCommand());
    commandLine.addSubcommand("depcoin-transfer", new DepCoinTransferCommand());
    commandLine.addSubcommand("depcoin-balance", new DepCoinBalanceCommand());
    commandLine.addSubcommand("ist-balance", new IstBalanceCommand());
    commandLine.addSubcommand("my-address", new WalletAddressCommand());
    commandLine.addSubcommand("ist-transfer", new IstCoinTransferCommand());
    commandLine.addSubcommand("contract-call", new ContractCallCommand());
    commandLine.addSubcommand("exit", new ExitCommand());
    commandLine.addSubcommand("help", new CommandLine.HelpCommand());
    commandLine.setOut(out);
    commandLine.setErr(err);
    commandLine.setExecutionExceptionHandler((exception, commandLineRef, parseResult) -> {
      err.println(Ansi.AUTO.string("@|bold,red error:|@ " + exception.getMessage()));
      logger.debug("Client command failed", exception);
      return commandLineRef.getCommandSpec().exitCodeOnExecutionException();
    });
    commandLine.setParameterExceptionHandler((exception, parseResult) -> {
      err.println(Ansi.AUTO.string("@|bold,yellow input error:|@ " + exception.getMessage()));
      exception.getCommandLine().usage(err);
      return exception.getCommandLine().getCommandSpec().exitCodeOnInvalidInput();
    });
    return commandLine;
  }

  boolean shouldExit() {
    return exitRequested.get();
  }

  @Command(name = "client-shell", mixinStandardHelpOptions = true, description = "Interactive DepChain client shell.")
  private final class RootCommand implements Runnable {
    @Override
    public void run() {
      commandLine().usage(out);
    }
  }

  @Command(name = "depcoin-transfer", description = "Transfer native DepCoin to another account.")
  private final class DepCoinTransferCommand implements Runnable {
    @Parameters(index = "0", description = "Recipient account address as 40 lowercase hex chars, without 0x.")
    private String recipientAddress;

    @Parameters(index = "1", description = "Native DepCoin amount to transfer.")
    private long amount;

    @Parameters(index = "2", description = "Transaction nonce for the client account.")
    private long nonce;

    @Parameters(index = "3", arity = "0..1", defaultValue = "" + DEFAULT_DEP_TRANSFER_GAS_LIMIT, description = "Gas limit. Default: ${DEFAULT-VALUE}.")
    private long gasLimit;

    @Parameters(index = "4", arity = "0..1", defaultValue = "" + DEFAULT_GAS_PRICE, description = "Gas price. Default: ${DEFAULT-VALUE}.")
    private long gasPrice;

    @Override
    public void run() {
      try {
        printTransactionResponse(client.requestDepCoinTransfer(recipientAddress, amount, nonce, gasLimit, gasPrice));
      } catch (IncoherentReplicaResponseException exception) {
        printIncoherentReplyError("DepCoin transfer");
      } catch (Exception exception) {
        err.println(Ansi.AUTO.string("@|bold,red error:|@ could not execute DepCoin transfer"));
        logger.error("Error executing DepCoin transfer", exception);
      }
    }
  }

  @Command(name = "depcoin-balance", description = "Query a native DepCoin balance.")
  private final class DepCoinBalanceCommand implements Runnable {
    @Parameters(index = "0", arity = "0..1", description = "Account address as 40 lowercase hex chars, without 0x. Defaults to the local wallet.")
    private String ownerAddress;

    @Override
    public void run() {
      try {
        printQueryResponse("depcoinBalance", ownerAddress == null ? client.requestOwnDepCoinBalance() : client.requestDepCoinBalance(ownerAddress));
      } catch (IncoherentReplicaResponseException exception) {
        printIncoherentReplyError("DepCoin balance query");
      } catch (Exception exception) {
        err.println(Ansi.AUTO.string("@|bold,red error:|@ could not query DepCoin balance"));
        logger.error("Error querying DepCoin balance", exception);
      }
    }
  }

  @Command(name = "ist-balance", description = "Query an account balance on the IST Coin contract.")
  private final class IstBalanceCommand implements Runnable {
    @Parameters(index = "0", arity = "0..1", description = "Account address as 40 lowercase hex chars, without 0x. Defaults to the local wallet.")
    private String ownerAddress;

    @Override
    public void run() {
      try {
        printQueryResponse("istBalance", ownerAddress == null ? client.requestOwnIstCoinBalance() : client.requestIstCoinBalance(ownerAddress));
      } catch (IncoherentReplicaResponseException exception) {
        printIncoherentReplyError("IST Coin balance query");
      } catch (Exception exception) {
        err.println(Ansi.AUTO.string("@|bold,red error:|@ could not query IST Coin balance"));
        logger.error("Error querying IST Coin balance", exception);
      }
    }
  }

  @Command(name = "my-address", description = "Print the local wallet address.")
  private final class WalletAddressCommand implements Runnable {
    @Override
    public void run() {
      out.println(Ansi.AUTO.string("@|bold,green ok|@ walletAddress=" + client.walletAddress()));
    }
  }

  @Command(name = "ist-transfer", description = "Transfer IST Coin tokens to another account.")
  private final class IstCoinTransferCommand implements Runnable {
    @Parameters(index = "0", description = "Recipient account address as 40 lowercase hex chars, without 0x.")
    private String recipientAddress;

    @Parameters(index = "1", description = "Raw token value to transfer.")
    private long rawValue;

    @Parameters(index = "2", description = "Transaction nonce for the client account.")
    private long nonce;

    @Parameters(index = "3", arity = "0..1", defaultValue = "" + DEFAULT_CONTRACT_CALL_GAS_LIMIT, description = "Gas limit. Default: ${DEFAULT-VALUE}.")
    private long gasLimit;

    @Parameters(index = "4", arity = "0..1", defaultValue = "" + DEFAULT_GAS_PRICE, description = "Gas price. Default: ${DEFAULT-VALUE}.")
    private long gasPrice;

    @Override
    public void run() {
      try {
        printBooleanResponse("transferResult", client.requestIstCoinTransfer(recipientAddress, rawValue, nonce, gasLimit, gasPrice));
      } catch (IncoherentReplicaResponseException exception) {
        printIncoherentReplyError("IST Coin transfer");
      } catch (Exception exception) {
        err.println(Ansi.AUTO.string("@|bold,red error:|@ could not execute IST Coin transfer"));
        logger.error("Error executing IST Coin transfer", exception);
      }
    }
  }

  @Command(name = "contract-call", description = "Execute a generic EVM contract call.")
  private final class ContractCallCommand implements Runnable {
    @Parameters(index = "0", description = "Contract address as 40 lowercase hex chars, without 0x.")
    private String contractAddress;

    @Parameters(index = "1", description = "Hex input payload, with or without 0x.")
    private String inputHex;

    @Parameters(index = "2", description = "Transaction nonce for the client account.")
    private long nonce;

    @Parameters(index = "3", arity = "0..1", defaultValue = "0", description = "Native DepCoin value to send. Default: ${DEFAULT-VALUE}.")
    private long amount;

    @Parameters(index = "4", arity = "0..1", defaultValue = "" + DEFAULT_CONTRACT_CALL_GAS_LIMIT, description = "Gas limit. Default: ${DEFAULT-VALUE}.")
    private long gasLimit;

    @Parameters(index = "5", arity = "0..1", defaultValue = "" + DEFAULT_GAS_PRICE, description = "Gas price. Default: ${DEFAULT-VALUE}.")
    private long gasPrice;

    @Override
    public void run() {
      try {
        byte[] input = decodeHex(inputHex);
        printTransactionResponse(client.requestContractCall(contractAddress, amount, nonce, gasLimit, gasPrice, input));
      } catch (IncoherentReplicaResponseException exception) {
        printIncoherentReplyError("contract call");
      } catch (Exception exception) {
        err.println(Ansi.AUTO.string("@|bold,red error:|@ could not execute contract call"));
        logger.error("Error executing generic contract call", exception);
      }
    }
  }

  @Command(name = "exit", description = "Exit the interactive client shell.")
  private final class ExitCommand implements Runnable {
    @Override
    public void run() {
      exitRequested.set(true);
    }
  }

  private void printTransactionResponse(pt.ulisboa.depchain.proto.TransactionResponse response) {
    if (response == null) {
      out.println(Ansi.AUTO.string("@|bold,yellow timeout|@ waiting for coherent replies"));
      return;
    }

    if (!response.hasReceipt()) {
      out.println(Ansi.AUTO.string("@|bold,yellow pending|@ " + response.getMessage()));
      return;
    }

    var receipt = response.getReceipt();
    if (receipt.getSuccess()) {
      StringBuilder success = new StringBuilder();
      success.append("@|bold,green ok|@ ").append(response.getMessage());
      success.append(" tx=").append(receipt.getTransactionHash());
      success.append(" gas=").append(receipt.getGasUsed());
      success.append(" node=").append(receipt.getNodeHash());
      if (receipt.hasReturnData()) {
        success.append(" return=0x").append(HexFormat.of().formatHex(receipt.getReturnData().toByteArray()));
      }
      out.println(Ansi.AUTO.string(success.toString()));
      return;
    }

    StringBuilder failure = new StringBuilder();
    failure.append("@|bold,yellow failed|@ ").append(response.getMessage());
    failure.append(" tx=").append(receipt.getTransactionHash());
    failure.append(" gas=").append(receipt.getGasUsed());
    if (receipt.hasErrorMessage()) {
      failure.append(" error=").append(receipt.getErrorMessage());
    }
    out.println(Ansi.AUTO.string(failure.toString()));
  }

  private void printQueryResponse(String label, pt.ulisboa.depchain.proto.QueryResponse response) {
    if (response == null) {
      out.println(Ansi.AUTO.string("@|bold,yellow timeout|@ waiting for coherent replies"));
      return;
    }

    if (!response.getSuccess()) {
      StringBuilder failure = new StringBuilder();
      failure.append("@|bold,yellow failed|@ ").append(response.getMessage());
      if (response.hasErrorMessage()) {
        failure.append(" error=").append(response.getErrorMessage());
      }
      out.println(Ansi.AUTO.string(failure.toString()));
      return;
    }

    if (!response.hasReturnData()) {
      throw new IllegalArgumentException("Successful query did not include return data");
    }

    BigInteger decoded = decodeUnsignedResult(response.getReturnData().toByteArray());
    out.println(Ansi.AUTO.string("@|bold,green ok|@ " + label + "=" + decoded));
  }

  private void printBooleanResponse(String label, pt.ulisboa.depchain.proto.TransactionResponse response) {
    if (!printSuccessfulTransactionWithReturnData(response)) {
      return;
    }

    boolean decoded = decodeBooleanResult(response.getReceipt().getReturnData().toByteArray());
    out.println(Ansi.AUTO
        .string("@|bold,green ok|@ " + label + "=" + decoded + " tx=" + response.getReceipt().getTransactionHash() + " gas=" + response.getReceipt().getGasUsed()));
  }

  private boolean printSuccessfulTransactionWithReturnData(pt.ulisboa.depchain.proto.TransactionResponse response) {
    if (response == null) {
      out.println(Ansi.AUTO.string("@|bold,yellow timeout|@ waiting for coherent replies"));
      return false;
    }

    if (!response.hasReceipt() || !response.getReceipt().getSuccess()) {
      printTransactionResponse(response);
      return false;
    }

    if (!response.getReceipt().hasReturnData()) {
      throw new IllegalArgumentException("Successful transaction did not include return data");
    }

    return true;
  }

  private void printIncoherentReplyError(String operation) {
    err.println(Ansi.AUTO.string("@|bold,red error:|@ replicas did not return a coherent majority for " + operation));
  }

  private static BigInteger decodeUnsignedResult(byte[] encoded) {
    if (encoded == null || encoded.length == 0) {
      throw new IllegalArgumentException("successful response did not include a numeric value");
    }
    return new BigInteger(1, encoded);
  }

  private static boolean decodeBooleanResult(byte[] encoded) {
    return !decodeUnsignedResult(encoded).equals(BigInteger.ZERO);
  }

  private static byte[] decodeHex(String inputHex) {
    String normalized = inputHex == null ? "" : inputHex.trim();
    if (normalized.isEmpty()) {
      throw new IllegalArgumentException("inputHex must not be blank");
    }
    if (!normalized.startsWith("0x") && !normalized.startsWith("0X")) {
      normalized = "0x" + normalized;
    }
    return HexFormat.of().parseHex(normalized.substring(2));
  }
}
