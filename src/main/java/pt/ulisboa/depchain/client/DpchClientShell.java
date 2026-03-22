package pt.ulisboa.depchain.client;

import java.io.PrintWriter;
import java.math.BigInteger;
import java.util.HexFormat;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Parameters;

public final class DpchClientShell {
  private static final Logger logger = LoggerFactory.getLogger(DpchClientShell.class);
  private static final String PROMPT = "depchain> ";
  private static final long DEFAULT_DEP_TRANSFER_GAS_LIMIT = 21_000L;
  private static final long DEFAULT_IST_CALL_GAS_LIMIT = 250_000L;
  private static final long DEFAULT_GAS_PRICE = 1L;

  private final DpchClient client;

  public DpchClientShell(DpchClient client) {
    this.client = client;
  }

  public void run() throws Exception {
    try (Scanner scanner = new Scanner(System.in); PrintWriter out = new PrintWriter(System.out, true); PrintWriter err = new PrintWriter(System.err, true)) {
      runInputLoop(scanner, out, err);
    }

    System.out.println("Bye.");
  }

  private void runInputLoop(Scanner scanner, PrintWriter out, PrintWriter err) {
    ClientShellCommand shell = new ClientShellCommand(client, out, err);
    CommandLine shellCommandLine = shell.commandLine();

    out.print(Ansi.AUTO.string("@|bold,cyan " + banner().stripTrailing() + "|@"));
    out.println();
    while (!shell.shouldExit()) {
      if (!scanner.hasNextLine()) {
        break;
      }

      out.print(Ansi.AUTO.string("@|bold,cyan " + PROMPT + "|@"));
      out.flush();
      String input = scanner.nextLine();
      if (input == null || input.isBlank()) {
        continue;
      }

      shellCommandLine.execute(shellArgs(input));
    }
  }

  static String[] shellArgs(String input) {
    String trimmedInput = input == null ? "" : input.trim();
    if (trimmedInput.isEmpty()) {
      return new String[0];
    }

    int firstWhitespace = firstWhitespaceIndex(trimmedInput);
    String commandToken = firstWhitespace >= 0 ? trimmedInput.substring(0, firstWhitespace) : trimmedInput;
    String normalizedCommand = commandToken.toLowerCase();

    return switch (normalizedCommand) {
      case "depcoin-transfer", "transfer", "t", "dep-transfer" -> tokenizeCommand(trimmedInput, firstWhitespace, "depcoin-transfer");
      case "ist-transfer", "it" -> tokenizeCommand(trimmedInput, firstWhitespace, "ist-transfer");
      case "depcoin-balance", "b", "dep-balance" -> tokenizeCommand(trimmedInput, firstWhitespace, "depcoin-balance");
      case "ist-balance", "ib" -> tokenizeCommand(trimmedInput, firstWhitespace, "ist-balance");
      case "exit", "quit", "q" -> new String[]{"exit"};
      case "help", "h", "?" -> tokenizeCommand(trimmedInput, firstWhitespace, "help");
      case "-h", "--help" -> new String[]{"--help"};
      default -> tokenizeCommand(trimmedInput, firstWhitespace, normalizedCommand);
    };
  }

  private static int firstWhitespaceIndex(String input) {
    for (int i = 0; i < input.length(); i++) {
      if (Character.isWhitespace(input.charAt(i))) {
        return i;
      }
    }
    return -1;
  }

  private static String[] tokenizeCommand(String trimmedInput, int firstWhitespace, String normalizedCommand) {
    if (firstWhitespace < 0) {
      return new String[]{normalizedCommand};
    }

    String remainder = trimmedInput.substring(firstWhitespace).trim();
    if (remainder.isEmpty()) {
      return new String[]{normalizedCommand};
    }

    String[] args = remainder.split("\\s+");
    String[] command = new String[args.length + 1];
    command[0] = normalizedCommand;
    System.arraycopy(args, 0, command, 1, args.length);
    return command;
  }

  private String banner() {
    return """
        DepChain Client
        Type 'depcoin-transfer <to> <amount> <nonce> [gasLimit] [gasPrice]' to transfer native DepCoin.
        Type 'depcoin-balance <owner>' to query native DepCoin balances.
        Type 'ist-balance <owner>' to query IST Coin balances.
        Type 'ist-transfer <to> <rawValue> <nonce> [gasLimit] [gasPrice]' to transfer IST Coin tokens.
        Type 'help' to list commands. Type 'exit' to quit.
        """;
  }

  @Command(name = "client-shell", mixinStandardHelpOptions = true, description = "Interactive DepChain client shell.")
  private static final class ClientShellCommand implements Runnable {
    private final DpchClient client;
    private final PrintWriter out;
    private final PrintWriter err;
    private final AtomicBoolean exitRequested = new AtomicBoolean();

    private ClientShellCommand(DpchClient client, PrintWriter out, PrintWriter err) {
      this.client = client;
      this.out = out;
      this.err = err;
    }

    private CommandLine commandLine() {
      CommandLine commandLine = new CommandLine(this);
      commandLine.addSubcommand("depcoin-transfer", new DepCoinTransferCommand());
      commandLine.addSubcommand("depcoin-balance", new DepCoinBalanceCommand());
      commandLine.addSubcommand("ist-balance", new IstBalanceCommand());
      commandLine.addSubcommand("ist-transfer", new IstCoinTransferCommand());
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

    private boolean shouldExit() {
      return exitRequested.get();
    }

    @Override
    public void run() {
      commandLine().usage(out);
    }

    @Command(name = "depcoin-transfer", aliases = {"transfer", "t", "dep-transfer"}, description = "Transfer native DepCoin to another account.")
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

    @Command(name = "depcoin-balance", aliases = {"b", "dep-balance"}, description = "Query a native DepCoin balance.")
    private final class DepCoinBalanceCommand implements Runnable {
      @Parameters(index = "0", description = "Account address as 40 lowercase hex chars, without 0x.")
      private String ownerAddress;

      @Override
      public void run() {
        try {
          printQueryResponse("depcoinBalance", client.requestDepCoinBalance(ownerAddress));
        } catch (IncoherentReplicaResponseException exception) {
          printIncoherentReplyError("DepCoin balance query");
        } catch (Exception exception) {
          err.println(Ansi.AUTO.string("@|bold,red error:|@ could not query DepCoin balance"));
          logger.error("Error querying DepCoin balance", exception);
        }
      }
    }

    @Command(name = "ist-balance", aliases = {"ib"}, description = "Query an account balance on the IST Coin contract.")
    private final class IstBalanceCommand implements Runnable {
      @Parameters(index = "0", description = "Account address as 40 lowercase hex chars, without 0x.")
      private String ownerAddress;

      @Override
      public void run() {
        try {
          printQueryResponse("istBalance", client.requestIstCoinBalance(ownerAddress));
        } catch (IncoherentReplicaResponseException exception) {
          printIncoherentReplyError("IST Coin balance query");
        } catch (Exception exception) {
          err.println(Ansi.AUTO.string("@|bold,red error:|@ could not query IST Coin balance"));
          logger.error("Error querying IST Coin balance", exception);
        }
      }
    }

    @Command(name = "ist-transfer", aliases = {"it"}, description = "Transfer IST Coin tokens to another account.")
    private final class IstCoinTransferCommand implements Runnable {
      @Parameters(index = "0", description = "Recipient account address as 40 lowercase hex chars, without 0x.")
      private String recipientAddress;

      @Parameters(index = "1", description = "Raw token value to transfer.")
      private long rawValue;

      @Parameters(index = "2", description = "Transaction nonce for the client account.")
      private long nonce;

      @Parameters(index = "3", arity = "0..1", defaultValue = "" + DEFAULT_IST_CALL_GAS_LIMIT, description = "Gas limit. Default: ${DEFAULT-VALUE}.")
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

    @Command(name = "exit", aliases = {"quit", "q"}, description = "Exit the interactive client shell.")
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

    private void printUnsignedResponse(String label, pt.ulisboa.depchain.proto.TransactionResponse response) {
      if (!printTransactionFailure(response)) {
        return;
      }

      BigInteger decoded = decodeUnsignedResult(response.getReceipt().getReturnData().toByteArray());
      out.println(Ansi.AUTO
          .string("@|bold,green ok|@ " + label + "=" + decoded + " tx=" + response.getReceipt().getTransactionHash() + " gas=" + response.getReceipt().getGasUsed()));
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
      if (!printTransactionFailure(response)) {
        return;
      }

      boolean decoded = decodeBooleanResult(response.getReceipt().getReturnData().toByteArray());
      out.println(Ansi.AUTO
          .string("@|bold,green ok|@ " + label + "=" + decoded + " tx=" + response.getReceipt().getTransactionHash() + " gas=" + response.getReceipt().getGasUsed()));
    }

    private boolean printTransactionFailure(pt.ulisboa.depchain.proto.TransactionResponse response) {
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
        throw new IllegalArgumentException("successful IST Coin balance response did not include a numeric value");
      }
      return new BigInteger(1, encoded);
    }

    private static boolean decodeBooleanResult(byte[] encoded) {
      return !decodeUnsignedResult(encoded).equals(BigInteger.ZERO);
    }
  }
}
