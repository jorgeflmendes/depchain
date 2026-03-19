package pt.ulisboa.depchain.client;

import java.nio.file.Path;
import java.util.concurrent.Callable;

import pt.ulisboa.depchain.shared.utils.ValidationUtils;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public final class Main {
  private Main() {
  }

  public static void main(String[] args) {
    int exitCode = new CommandLine(new ClientCommand()).execute(args);
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

  @Command(name = "client", mixinStandardHelpOptions = true, description = "Run the DepChain interactive client.")
  private static final class ClientCommand implements Callable<Integer> {
    @Option(names = {"-c", "--config"}, description = "Path to the cluster configuration file.")
    private Path configOption;

    @Parameters(index = "0", arity = "0..1", hidden = true, description = "Path to the cluster configuration file.")
    private Path configParameter;

    @Override
    public Integer call() throws Exception {
      Path configPath = configParameter;
      if (configOption != null) {
        configPath = configOption;
      }
      ValidationUtils.requireNonNull(configPath, "config");

      try (DpchClient client = DpchClient.open(configPath.toString())) {
        new DpchClientShell(client).run();
      }
      return 0;
    }
  }
}
