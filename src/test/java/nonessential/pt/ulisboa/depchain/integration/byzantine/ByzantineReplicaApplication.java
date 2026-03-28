package pt.ulisboa.depchain.integration.byzantine;

import java.nio.file.Path;
import java.util.concurrent.Callable;

import pt.ulisboa.depchain.shared.validation.ValidationUtils;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public final class ByzantineReplicaApplication {
  private ByzantineReplicaApplication() {
  }

  public static void main(String[] args) {
    int exitCode = new CommandLine(new ByzantineReplicaCommand()).execute(args);
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

  @Command(name = "byzantine-replica", mixinStandardHelpOptions = true, description = "Run a byzantine test replica with the requested attack mode.")
  private static final class ByzantineReplicaCommand implements Callable<Integer> {
    @Option(names = {"-i", "--replica-id", "--id"}, description = "Configured replica identifier to use.")
    private String replicaIdOption;

    @Option(names = {"-c", "--config"}, description = "Path to the cluster configuration file.")
    private Path configOption;

    @Option(names = {"-a", "--attack-mode"}, description = "Byzantine attack mode to activate.")
    private ByzantineAttackMode attackModeOption;

    @Parameters(index = "0", arity = "0..1", hidden = true, description = "Replica identifier from the configuration file.")
    private String replicaIdParameter;

    @Parameters(index = "1", arity = "0..1", hidden = true, description = "Path to the cluster configuration file.")
    private Path configParameter;

    @Parameters(index = "2", arity = "0..1", hidden = true, description = "Byzantine attack mode to activate.")
    private ByzantineAttackMode attackModeParameter;

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
      ByzantineAttackMode attackMode = attackModeParameter;
      if (attackModeOption != null) {
        attackMode = attackModeOption;
      }
      ValidationUtils.requireNonBlank(replicaId, "replicaId");
      ValidationUtils.requireNonNull(configPath, "config");
      ValidationUtils.requireNonNull(attackMode, "attackMode");

      ByzantineReplicaNode replicaNode = new ByzantineReplicaNode(replicaId, configPath.toString(), attackMode);
      replicaNode.run();
      return 0;
    }
  }
}
