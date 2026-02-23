package pt.ulisboa.depchain.client;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import pt.ulisboa.depchain.shared.config.ConfigFile;
import pt.ulisboa.depchain.shared.udp.DatagramTransport;

public final class Main {
  private Main() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: Main <value> <targetReplicaId> <configPath>");
      System.exit(1);
    }

    String value = args[0];
    String targetReplicaId = args[1];
    String configPath = args[2];

    ConfigFile config = ConfigFile.load(Path.of(configPath));
    ConfigFile.ReplicaSection targetReplica = config.requireReplica(targetReplicaId);

    try (DatagramTransport transport = DatagramTransport.unbound(config.client().requestTimeoutMs())) {
      byte[] requestBytes = value.getBytes(StandardCharsets.UTF_8);
      transport.send(targetReplica.host(), targetReplica.clientPort(), requestBytes);

      DatagramTransport.ReceivedDatagram responseDatagram =
          transport.receive(config.network().maxPacketSize());

      String response = new String(responseDatagram.payload(), StandardCharsets.UTF_8);
      System.out.println("response = " + response);
    }
  }
}
