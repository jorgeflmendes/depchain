package pt.ulisboa.depchain.client;

import pt.ulisboa.depchain.shared.logging.Logger;

public final class Main {
  private static final Logger logger = new Logger("ClientMain");

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      logger.error("Usage: Main <targetReplicaId> <configPath>");
      System.exit(1);
    }

    String targetReplicaId = args[0];
    String configPath = args[1];

    DpchClient client = new DpchClient(targetReplicaId, configPath);
    client.run();
  }
}
