package pt.ulisboa.depchain.client.cli;

import java.io.PrintWriter;
import java.util.Scanner;

import pt.ulisboa.depchain.client.api.ClientReplicaApi;

import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;

public final class ClientShell {
  private static final String PROMPT = "depchain> ";
  private static final String DIVIDER = "============================================================";
  private static final Ansi SHELL_ANSI = Ansi.ON;

  private final ClientReplicaApi client;

  public ClientShell(ClientReplicaApi client) {
    this.client = client;
  }

  public void run() throws Exception {
    Scanner scanner = new Scanner(System.in);
    PrintWriter out = new PrintWriter(System.out, true);
    PrintWriter err = new PrintWriter(System.err, true);
    runInputLoop(scanner, out, err);
    out.println(SHELL_ANSI.string("@|faint " + DIVIDER + "|@"));
    out.println(SHELL_ANSI.string("@|bold,cyan Session closed.|@"));
  }

  private void runInputLoop(Scanner scanner, PrintWriter out, PrintWriter err) {
    ClientShellCommandSet commandSet = new ClientShellCommandSet(client, out, err);
    CommandLine shellCommandLine = commandSet.commandLine();

    out.println(SHELL_ANSI.string(renderBanner().stripTrailing()));
    out.println(SHELL_ANSI.string("@|faint Type 'help' to list commands or 'exit' to quit.|@"));
    out.println();
    while (!commandSet.shouldExit()) {
      out.print(SHELL_ANSI.string(prompt()));
      out.flush();
      if (!scanner.hasNextLine()) {
        break;
      }
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

  private String prompt() {
    return "@|bold,cyan " + PROMPT + "|@";
  }

  private String renderBanner() {
    String walletAddress = client.getWalletAddress();
    return """
        @|faint %s|@
        @|bold,cyan DEPCHAIN CLIENT CONSOLE|@
        @|bold,green Active Wallet|@  %s
        @|bold,white Available Commands|@
          @|bold depcoin-transfer|@ <to> <amount> <nonce> [gasLimit] [gasPrice]
          @|bold depcoin-balance|@ [owner]
          @|bold ist-transfer|@ <to> <rawValue> <nonce> [gasLimit] [gasPrice]
          @|bold ist-balance|@ [owner]
          @|bold contract-call|@ <to> <inputHex> <nonce> [amount] [gasLimit] [gasPrice]
          @|bold my-address|@
        @|faint %s|@
        """.formatted(DIVIDER, walletAddress, DIVIDER);
  }
}
