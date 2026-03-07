package pt.ulisboa.depchain.client;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.links.handshaked.HandshakedPerfectLink;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.TimeUtil;

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

    // Resolve the target replica's address and port from the configuration, and get the request timeout.
    InetSocketAddress targetEndpoint =
        new InetSocketAddress(
            InetAddress.getByName(targetReplicaConfig.host()), targetReplicaConfig.clientPort());
    long timeoutMs = config.client().requestTimeoutMs();

    try (HandshakedPerfectLink transport = HandshakedPerfectLink.unbound()) {
      String responsePayload = sendRequest(transport, value, targetEndpoint, timeoutMs);
      System.out.println("response = " + responsePayload);
    } catch (IOException | IllegalStateException exception) {
      System.out.printf("Packet exchange error = %s%n", exception.getMessage());
      System.exit(2);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      System.out.printf("Packet exchange interrupted = %s%n", interrupted.getMessage());
      System.exit(2);
    }
  }

  // Sends one request and waits for the matching response.
  private static String sendRequest(
      HandshakedPerfectLink transport,
      String value,
      InetSocketAddress targetEndpoint,
      long timeoutMs)
      throws IOException, InterruptedException {
    long connectionId = ThreadLocalRandom.current().nextLong();
    transport.send(connectionId, value.getBytes(StandardCharsets.UTF_8), targetEndpoint);

    try {
      long deadlineMs = TimeUtil.deadlineAfter(timeoutMs);

      while (true) {
        long remainingMs = TimeUtil.remainingMsUntil(deadlineMs);
        if (remainingMs <= 0L) {
          throw new IOException("Request timed out after " + timeoutMs + " ms");
        }

        InboundPacket inbound = transport.receive(remainingMs);
        if (inbound == null) {
          throw new IOException("Request timed out after " + timeoutMs + " ms");
        }

        Dpch candidate = inbound.packet();
        if (candidate.connectionId() == connectionId) {
          return new String(candidate.payload(), StandardCharsets.UTF_8);
        }
      }
    } finally {
      try {
        transport.closeConnection(connectionId, targetEndpoint);
      } catch (RuntimeException ignored) {
        // Best-effort close.
      }
    }
  }
}


