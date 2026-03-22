package pt.ulisboa.depchain.client.shell;

import java.io.PrintWriter;
import java.util.Scanner;

import pt.ulisboa.depchain.client.DpchClient;

import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;

public final class DpchClientShell {
  private static final String PROMPT = "depchain> ";

  private final DpchClient client;

  public DpchClientShell(DpchClient client) {
    this.client = client;
  }

  public void run() throws Exception {
    Scanner scanner = new Scanner(System.in);
    PrintWriter out = new PrintWriter(System.out, true);
    PrintWriter err = new PrintWriter(System.err, true);
    runInputLoop(scanner, out, err);
    System.out.println("Bye.");
  }

  private void runInputLoop(Scanner scanner, PrintWriter out, PrintWriter err) {
    ClientShellCommandSet commandSet = new ClientShellCommandSet(client, out, err);
    CommandLine shellCommandLine = commandSet.commandLine();

    out.print(Ansi.AUTO.string("@|bold,cyan " + banner().stripTrailing() + "|@"));
    out.println();
    while (!commandSet.shouldExit()) {
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
    return ClientShellTokenizer.shellArgs(input);
  }

  private String banner() {
    return """
        DepChain Client
        Wallet address: %s
        Type 'depcoin-transfer <to> <amount> <nonce> [gasLimit] [gasPrice]' to transfer native DepCoin.
        Type 'depcoin-balance [owner]' to query native DepCoin balances.
        Type 'ist-balance [owner]' to query IST Coin balances.
        Type 'my-address' to print the local wallet address.
        Type 'ist-transfer <to> <rawValue> <nonce> [gasLimit] [gasPrice]' to transfer IST Coin tokens.
        Type 'contract-call <to> <inputHex> <nonce> [amount] [gasLimit] [gasPrice]' to call an EVM contract directly.
        Type 'help' to list commands. Type 'exit' to quit.
        """.formatted(client.walletAddress());
  }
}
