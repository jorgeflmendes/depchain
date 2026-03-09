package pt.ulisboa.depchain.server;

public final class Main {
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: Main <serverId> <configPath>");
      System.exit(1);
    }

    String serverId = args[0];
    String configPath = args[1];

    DpchServer server = new DpchServer(serverId, configPath);
    server.run();
  }
}
