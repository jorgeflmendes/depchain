package pt.ulisboa.depchain.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Main {
  private static final Logger logger = LoggerFactory.getLogger(Main.class);

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
