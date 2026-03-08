package pt.ulisboa.depchain.server;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.keys.PrivateKeyLoader;
import pt.ulisboa.depchain.shared.keys.PublicKeyLoader;
import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;

public final class Main {
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: Main <serverId> <configPath>");
      System.exit(1);
    }

    String serverId = args[0];
    String configPath = args[1];

    // Load the server configuration from the specified file path.
    ConfigParser config = ConfigParser.load(Path.of(configPath));
    ConfigParser.ReplicaSection replicaConfig = config.requireReplica(serverId);
    Map<Long, PublicKey> staticPKeys = PublicKeyLoader.loadStaticPublicKeys(config);
    PrivateKey localStaticSKey = PrivateKeyLoader.loadReplicaPrivateKey(config, replicaConfig.senderId());

    // Resolve the replica's bind address and port from the configuration.
    InetSocketAddress bindEndpoint = new InetSocketAddress(InetAddress.getByName(replicaConfig.host()), replicaConfig.clientPort());

    // Virtual threads to handle each request concurrently without blocking OS threads.
    ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();

    try (workers; AuthenticatedLink transport = AuthenticatedLink.bind(bindEndpoint, replicaConfig.senderId(), localStaticSKey, staticPKeys)) {
      System.out.printf("Replica %s listening for client UDP requests on %s:%d (config: %s)%n", replicaConfig.id(), replicaConfig.host(), replicaConfig.clientPort(), configPath);

      while (true) {
        InboundPacket request = transport.receive();
        if (request == null) {
          continue;
        }
        workers.submit(() -> handleRequest(transport, request.packet(), request.sender()));
      }
    }
  }

  // Request handler that just echoes back the received value
  private static void handleRequest(AuthenticatedLink transport, Dpch inbound, InetSocketAddress sender) {
    String senderText = sender.getAddress().getHostAddress() + ":" + sender.getPort();
    try {
      String payloadText = new String(inbound.payload(), StandardCharsets.UTF_8);
      byte[] responsePayload = ("Received " + payloadText).getBytes(StandardCharsets.UTF_8);
      transport.send(inbound.connectionId(), responsePayload, sender);
    } catch (RuntimeException exception) {
      System.out.printf("Packet exchange error while handling conn=%s seq=%d from %s = %s%n", inbound.connectionId(), inbound.sequenceNumber(), senderText, exception.getMessage());
    } finally {
      try {
        transport.closeConnection(inbound.connectionId(), sender);
      } catch (RuntimeException closeError) {
        System.out.printf("Close error while handling conn=%s seq=%d from %s = %s%n", inbound.connectionId(), inbound.sequenceNumber(), senderText, closeError.getMessage());
      }
    }
  }
}
