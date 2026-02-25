package pt.ulisboa.depchain.client;

import java.net.InetAddress;
import java.nio.file.Path;
import java.util.UUID;

import pt.ulisboa.depchain.shared.config.ConfigFile;
import pt.ulisboa.depchain.shared.udp.UdpRequestResponseTransport;
import pt.ulisboa.depchain.shared.udp.messages.MessageRequest;
import pt.ulisboa.depchain.shared.udp.messages.MessageResponse;

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
    InetAddress targetAddress = InetAddress.getByName(targetReplica.host());

    try (UdpRequestResponseTransport transport = UdpRequestResponseTransport.unbound(config.client().requestTimeoutMs(), config.network().maxPacketSize())) {
      // send a request to the target replica and wait for the response
      MessageRequest request = new MessageRequest(UUID.randomUUID(), value);
      MessageResponse response = transport.sendRequest(request, targetAddress, targetReplica.clientPort());

      if (!response.success()) {
        System.err.println("request failed = " + response.payload());
        System.exit(2);
      }

      System.out.println("response = " + String.valueOf(response.payload()));
    }
  }
}
