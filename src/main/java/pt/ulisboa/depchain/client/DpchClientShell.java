package pt.ulisboa.depchain.client;

import java.io.PrintWriter;
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
  private static final String BANNER = """
      DepChain Client
      Type 'append <value>' to submit a request.
      Type 'help' to list commands. Type 'exit' to quit.
      """;

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

    out.print(Ansi.AUTO.string("@|bold,cyan " + BANNER.stripTrailing() + "|@"));
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
    String trimmedInput = "";
    if (input != null) {
      trimmedInput = input.trim();
    }
    if (trimmedInput.isEmpty()) {
      return new String[0];
    }

    int firstWhitespace = firstWhitespaceIndex(trimmedInput);
    String commandToken = trimmedInput;
    if (firstWhitespace >= 0) {
      commandToken = trimmedInput.substring(0, firstWhitespace);
    }
    String normalizedCommand = commandToken.toLowerCase();

    return switch (normalizedCommand) {
      case "append", "a" -> {
        String value = "";
        if (firstWhitespace >= 0) {
          value = trimmedInput.substring(firstWhitespace).trim();
        }
        if (value.isEmpty()) {
          yield new String[]{"append"};
        }
        yield new String[]{"append", value};
      }
      case "exit", "quit", "q" -> new String[]{"exit"};
      case "help", "h", "?" -> {
        String target = "";
        if (firstWhitespace >= 0) {
          target = trimmedInput.substring(firstWhitespace).trim();
        }
        if (target.isEmpty()) {
          yield new String[]{"help"};
        }
        yield new String[]{"help", target};
      }
      case "-h", "--help" -> new String[]{"--help"};
      default -> new String[]{"append", trimmedInput};
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
      commandLine.addSubcommand("append", new AppendCommand());
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

    @Command(name = "append", aliases = {"a"}, description = "Broadcast an append request to the cluster.")
    private final class AppendCommand implements Runnable {
      @Parameters(index = "0", arity = "1", description = "Value to append.")
      private String value;

      @Override
      public void run() {
        try {
          String response = client.requestAppend(value);
          if (response == null) {
            out.println(Ansi.AUTO.string("@|bold,yellow timeout|@ waiting for coherent replies"));
            return;
          }
          out.println(Ansi.AUTO.string("@|bold,green ok|@ " + response));
        } catch (Exception exception) {
          err.println(Ansi.AUTO.string("@|bold,red error:|@ could not broadcast append request"));
          logger.error("Error broadcasting append request", exception);
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
  }
}
