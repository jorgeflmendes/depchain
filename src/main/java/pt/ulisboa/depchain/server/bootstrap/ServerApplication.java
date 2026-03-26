package pt.ulisboa.depchain.server.bootstrap;

import java.nio.file.Path;
import java.util.concurrent.Callable;

import pt.ulisboa.depchain.server.node.ReplicaNode;
import pt.ulisboa.depchain.shared.validation.ValidationUtils;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public final class ServerApplication {
  private ServerApplication() {
  }

  public static void main(String[] args) {
    int exitCode = new CommandLine(new ServerCommand()).execute(args);
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

  @Command(name = "server", mixinStandardHelpOptions = true, description = "Run a DepChain replica node.")
  private static final class ServerCommand implements Callable<Integer> {
    @Option(names = {"-i", "--replica-id", "--id"}, description = "Configured replica identifier to use.")
    private String replicaIdOption;

    @Option(names = {"-c", "--config"}, description = "Path to the cluster configuration file.")
    private Path configOption;

    @Parameters(index = "0", arity = "0..1", hidden = true, description = "Replica identifier from the configuration file.")
    private String replicaIdParameter;

    @Parameters(index = "1", arity = "0..1", hidden = true, description = "Path to the cluster configuration file.")
    private Path configParameter;

    @Override
    public Integer call() throws Exception {
      String replicaId = replicaIdParameter;
      if (replicaIdOption != null) {
        replicaId = replicaIdOption;
      }
      Path configPath = configParameter;
      if (configOption != null) {
        configPath = configOption;
      }
      ValidationUtils.requireNonBlank(replicaId, "replicaId");
      ValidationUtils.requireNonNull(configPath, "config");

      ReplicaNode replicaNode = new ReplicaNode(replicaId, configPath.toString());
      replicaNode.run();
      return 0;
    }
  }
}
