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

public final class DpchServer {
  private final ConfigParser configParser;
  private final ConfigParser.ReplicaSection replicaConfig;
  private final PrivateKey localStaticSKey;
  private final Map<Long, PublicKey> staticPKeys;
  private final String configPath;

  public DpchServer(String serverId, String configPath) throws Exception {
    this.configParser = ConfigParser.load(Path.of(configPath));
    this.replicaConfig = configParser.requireReplica(serverId);
    this.localStaticSKey = PrivateKeyLoader.loadReplicaPrivateKey(configParser, replicaConfig.senderId());
    this.staticPKeys = PublicKeyLoader.loadStaticPublicKeys(configParser);
    this.configPath = configPath;
  }

  public void run() throws Exception {
    InetSocketAddress bindEndpoint = new InetSocketAddress(InetAddress.getByName(replicaConfig.host()), replicaConfig.clientPort());
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

  private static void handleRequest(AuthenticatedLink transport, Dpch inbound, InetSocketAddress sender) {
    String senderText = sender.getAddress().getHostAddress() + ":" + sender.getPort();
    try {
      String payloadText = new String(inbound.payload(), StandardCharsets.UTF_8);

      System.out.printf("Received request from %s%n", sender);
      byte[] responsePayload = ("Received " + payloadText).getBytes(StandardCharsets.UTF_8);
      transport.send(inbound.connectionId(), responsePayload, sender);
    } catch (RuntimeException exception) {
      System.out.printf("Packet exchange error from %s = %s%n", senderText, exception.getMessage());
    } finally {
      try {
        transport.closeConnection(inbound.connectionId(), sender);
      } catch (RuntimeException closeError) {
        System.out.printf("Close error from %s = %s%n", senderText, closeError.getMessage());
      }
    }
  }
}
