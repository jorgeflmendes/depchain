package pt.ulisboa.depchain.client.bootstrap;

import java.nio.file.Path;
import java.util.concurrent.Callable;

import pt.ulisboa.depchain.client.api.ClientReplicaApi;
import pt.ulisboa.depchain.client.cli.ClientShell;
import pt.ulisboa.depchain.shared.validation.ValidationUtils;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public final class ClientApplication {
  private ClientApplication() {
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

    @Option(names = {"-i", "--client-id", "--id"}, required = true, description = "Configured client identifier to use.")
    private String clientIdOption;

    @Parameters(index = "0", arity = "0..1", hidden = true, description = "Path to the cluster configuration file.")
    private Path configParameter;

    @Override
    public Integer call() throws Exception {
      Path configPath = configOption != null ? configOption : configParameter;
      ValidationUtils.requireNonBlank(clientIdOption, "clientId");
      ValidationUtils.requireNonNull(configPath, "config");

      try (ClientReplicaApi client = ClientReplicaApi.connect(configPath.toString(), clientIdOption)) {
        new ClientShell(client).run();
      }

      return 0;
    }
  }
}
