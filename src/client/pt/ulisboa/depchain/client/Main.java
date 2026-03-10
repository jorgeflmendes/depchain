package pt.ulisboa.depchain.client;

public final class Main {
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: Main <targetReplicaId> <configPath>");
      System.exit(1);
    }

    String targetReplicaId = args[0];
    String configPath = args[1];

    DpchClient client = new DpchClient(targetReplicaId, configPath);
    client.run();
  }
}
