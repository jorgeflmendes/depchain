package pt.ulisboa.depchain.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Main {
  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      logger.error("Usage: Main <serverId> <configPath>");
      System.exit(1);
    }

    String serverId = args[0];
    String configPath = args[1];

    DpchServer server = new DpchServer(serverId, configPath);
    server.run();
  }
}
