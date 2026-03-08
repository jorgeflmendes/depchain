package pt.ulisboa.depchain.client;

public final class Main {
  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: Main <value> <targetReplicaId> <configPath>");
      System.exit(1);
    }

    String value = args[0];
    String targetReplicaId = args[1];
    String configPath = args[2];

    DpchClient client = new DpchClient(configPath);

    String response = client.append(value, targetReplicaId);

    System.out.println("response = " + response);
  }
}
