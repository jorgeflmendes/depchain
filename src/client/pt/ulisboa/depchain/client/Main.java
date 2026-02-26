package pt.ulisboa.depchain.client;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;

import pt.ulisboa.depchain.shared.config.ConfigFile;
import pt.ulisboa.depchain.shared.links.fairloss.message.Dpch;
import pt.ulisboa.depchain.shared.links.fairloss.transport.FairLossLink;

public final class Main {
  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: Main <value> <targetReplicaId> <configPath>");
      System.exit(1);
    }

    String value = args[0];
    String targetReplicaId = args[1];
    String configPath = args[2];

    try {
      ConfigFile config = ConfigFile.load(Path.of(configPath));
      ConfigFile.ReplicaSection targetReplica = config.requireReplica(targetReplicaId);
      InetAddress targetAddress = InetAddress.getByName(targetReplica.host());

      try (FairLossLink transport = FairLossLink.unbound(config.client().requestTimeoutMs(), config.network().maxPacketSize())) {
        int connectionId = ThreadLocalRandom.current().nextInt(); // TODO: it should be an uuid or something else, but for now let's just use a random int, maybe it wont be needed
        int sequenceNumber = 0;

        // Create and send request, and wait for response
        Dpch request = Dpch.data(connectionId, sequenceNumber, value.getBytes(StandardCharsets.UTF_8));
        Dpch response = transport.sendRequest(request, targetAddress, targetReplica.clientPort());

        String responsePayload = new String(response.payload(), StandardCharsets.UTF_8);
        System.out.println("response = " + responsePayload);
      }
    } catch (Exception exception) {
      System.out.printf("Packet exchange error = %s%n", exception.getMessage());
      exception.printStackTrace(System.out);
      System.exit(2);
    }
  }
}
