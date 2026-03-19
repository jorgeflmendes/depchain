package pt.ulisboa.depchain.server;

import java.nio.file.Path;
import java.util.concurrent.Callable;

import pt.ulisboa.depchain.server.runtime.ReplicaServer;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public final class Main {
  private Main() {
  }

  public static void main(String[] args) {
    int exitCode = new CommandLine(new ServerCommand()).execute(args);
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

  @Command(name = "server", mixinStandardHelpOptions = true, description = "Run a DepChain replica server.")
  private static final class ServerCommand implements Callable<Integer> {
    @Option(names = {"-i", "--id"}, description = "Replica identifier from the configuration file.")
    private String serverIdOption;

    @Option(names = {"-c", "--config"}, description = "Path to the cluster configuration file.")
    private Path configOption;

    @Parameters(index = "0", arity = "0..1", hidden = true, description = "Replica identifier from the configuration file.")
    private String serverIdParameter;

    @Parameters(index = "1", arity = "0..1", hidden = true, description = "Path to the cluster configuration file.")
    private Path configParameter;

    @Override
    public Integer call() throws Exception {
      String serverId = serverIdParameter;
      if (serverIdOption != null) {
        serverId = serverIdOption;
      }
      Path configPath = configParameter;
      if (configOption != null) {
        configPath = configOption;
      }
      ValidationUtils.requireNonBlank(serverId, "id");
      ValidationUtils.requireNonNull(configPath, "config");

      ReplicaServer server = new ReplicaServer(serverId, configPath.toString());
      server.run();
      return 0;
    }
  }
}
