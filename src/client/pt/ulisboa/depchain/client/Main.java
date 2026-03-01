package pt.ulisboa.depchain.client;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;

import pt.ulisboa.depchain.shared.config.ConfigFile;
import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.links.stubborn.StubbornLink;
import pt.ulisboa.depchain.shared.network.messages.InboundMessage;

public final class Main {
  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: Main <value> <targetReplicaId> <configPath>");
      System.exit(1);
    }

    String value = args[0];
    String targetReplicaId = args[1];
    String configPath = args[2];

    // Load the client configuration from the specified file path.
    ConfigFile config = ConfigFile.load(Path.of(configPath));
    ConfigFile.ReplicaSection targetReplicaConfig = config.requireReplica(targetReplicaId);
    ConfigFile.StubbornSection stubbornConfig = config.stubborn();
    InetAddress targetAddress = InetAddress.getByName(targetReplicaConfig.host());

    try {
      try (StubbornLink transport = StubbornLink.unbound(config.network().maxPacketSize(), stubbornConfig.baseDelayMs(), stubbornConfig.maxDelayMs(), stubbornConfig.jitterRatio(), stubbornConfig.maxPending(), stubbornConfig.heapCompactMinSize())) {
        int connectionId = ThreadLocalRandom.current().nextInt(); // TODO: it should be an uuid or something else, but for now let's just use a random int, maybe it wont be needed
        int sequenceNumber = 0;

        // Create and send one packet over the stubborn link.
        Dpch request = Dpch.data(connectionId, sequenceNumber, value.getBytes(StandardCharsets.UTF_8));
        var trackedKey = transport.sendTracked(request, targetAddress, targetReplicaConfig.clientPort());

        // Wait for the response packet with the same connectionId and sequenceNumber as the original request.
        Dpch response;
        while (true) {
          InboundMessage inbound = transport.receive();
          Dpch candidate = inbound.packet();
          
          if (candidate.connectionId() == request.connectionId() && candidate.sequenceNumber() == request.sequenceNumber()) {
            response = candidate;
            break;
          }
        }

        // Response arrived: stop retrying this request.
        transport.cancelTracked(trackedKey, targetAddress, targetReplicaConfig.clientPort());

        String responsePayload = new String(response.payload(), StandardCharsets.UTF_8);
        System.out.println("response = " + responsePayload);
      }
    } catch (Exception exception) {
      System.out.printf("Packet exchange error = %s%n", exception.getMessage());
      System.exit(2);
    }
  }
}
