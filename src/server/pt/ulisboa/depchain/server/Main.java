package pt.ulisboa.depchain.server;

import pt.ulisboa.depchain.shared.logging.Logger;

public final class Main {
  private static final Logger logger = new Logger("ServerMain");

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
