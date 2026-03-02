package pt.ulisboa.depchain.client;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.links.perfect.PerfectLink;
import pt.ulisboa.depchain.shared.network.model.InboundMessage;

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
    ConfigParser config = ConfigParser.load(Path.of(configPath));
    ConfigParser.ReplicaSection targetReplicaConfig = config.requireReplica(targetReplicaId);
    ConfigParser.StubbornSection stubbornConfig = config.stubborn();
    ConfigParser.PerfectSection perfectConfig = config.perfect();
    InetAddress targetAddress = InetAddress.getByName(targetReplicaConfig.host());

    try {
      // TODO: wtf do we need all these parameters?
      try (PerfectLink perfectLink = PerfectLink.unbound(config.network().maxPacketSize(), stubbornConfig.baseDelayMs(), stubbornConfig.maxDelayMs(), stubbornConfig.jitterRatio(), stubbornConfig.maxPending(), stubbornConfig.heapCompactMinSize(), stubbornConfig.maxRetryAttempts(), stubbornConfig.maxTrackedLifetimeMs(), perfectConfig.maxWindowSize(), perfectConfig.maxStreamStates(), perfectConfig.streamIdleTtlMs())) {
        int connectionId = ThreadLocalRandom.current().nextInt(); // TODO: it should be an uuid or something else, but for now let's just use a random int, maybe it wont be needed

        // Create and send one packet over the perfect link.
        perfectLink.sendReliable(connectionId, value.getBytes(StandardCharsets.UTF_8), targetAddress, targetReplicaConfig.clientPort());

        // Wait for a response packet with the same connectionId.
        Dpch response;
        while (true) {
          InboundMessage inbound = perfectLink.receive();
          Dpch candidate = inbound.packet();
          
          if (candidate.connectionId() == connectionId) {
            response = candidate;
            break;
          }
        }

        String responsePayload = new String(response.payload(), StandardCharsets.UTF_8);
        System.out.println("response = " + responsePayload);
      }
    } catch (Exception exception) {
      System.out.printf("Packet exchange error = %s%n", exception.getMessage());
      System.exit(2);
    }
  }
}
