package pt.ulisboa.depchain.server;

public final class Main {
  private static final pt.ulisboa.depchain.shared.logging.Logger logger = new pt.ulisboa.depchain.shared.logging.Logger("ServerMain");

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
