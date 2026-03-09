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
  private final Replica replica;

  public DpchServer(String serverId, String configPath) throws Exception {
    this.configParser = ConfigParser.load(Path.of(configPath));
    this.replicaConfig = configParser.requireReplica(serverId);
    this.replica = new Replica(Math.toIntExact(replicaConfig.senderId()), configParser.system().n());
    this.localStaticSKey = PrivateKeyLoader.loadReplicaPrivateKey(configParser, replicaConfig.senderId());
    this.staticPKeys = PublicKeyLoader.loadStaticPublicKeys(configParser);
  }

  public void run() throws Exception {
    InetSocketAddress clientBindEndpoint = new InetSocketAddress(InetAddress.getByName(replicaConfig.host()), replicaConfig.clientPort());
    InetSocketAddress nodeBindEndpoint = new InetSocketAddress(InetAddress.getByName(replicaConfig.host()), replicaConfig.consensusPort());
    ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();

    try (workers;
        AuthenticatedLink clientTransport = AuthenticatedLink.bind(clientBindEndpoint, replicaConfig.senderId(), localStaticSKey, staticPKeys);
        AuthenticatedLink nodeListener = AuthenticatedLink.bind(nodeBindEndpoint, replicaConfig.senderId(), localStaticSKey, staticPKeys); // Bind for receiving
        AuthenticatedLink nodeTransport = AuthenticatedLink.unbound(replicaConfig.senderId(), localStaticSKey, staticPKeys)) { // Unbound for sending
      System.out.printf("Replica %s client listener: %s:%d%n", replicaConfig.id(), replicaConfig.host(), replicaConfig.clientPort());
      System.out.printf("Replica %s node listener: %s:%d%n", replicaConfig.id(), replicaConfig.host(), replicaConfig.consensusPort());

      // Keep client and replica traffic on their own listeners.
      workers.submit(() -> runClientLoop(clientTransport, workers));
      workers.submit(() -> runNodeLoop(nodeListener, nodeTransport, workers));
      Thread.currentThread().join();
    }
  }

  private static void runClientLoop(AuthenticatedLink transport, ExecutorService workers) {
    while (true) {
      InboundPacket request = receiveNextInbound(transport);
      if (request == null) {
        return;
      }

      workers.submit(() -> handleClientRequest(transport, request.packet(), request.sender()));
    }
  }

  private static void runNodeLoop(AuthenticatedLink listener, AuthenticatedLink transport, ExecutorService workers) {
    while (true) {
      InboundPacket request = receiveNextInbound(listener);
      if (request == null) {
        return;
      }

      workers.submit(() -> handleNodeRequest(transport, request.packet(), request.sender()));
    }
  }

  private static InboundPacket receiveNextInbound(AuthenticatedLink transport) {
    try {
      return transport.receive();
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  private static void handleClientRequest(AuthenticatedLink transport, Dpch inbound, InetSocketAddress sender) {
    String senderText = sender.getAddress().getHostAddress() + ":" + sender.getPort();
    System.out.printf("Client request from %s%n", sender);

    try {
      // TODO: client request handling logic goes here. broadcast, etc.

      // Ex.: Deserialize the received request payload as a UTF-8 string.
      String requestText = new String(inbound.payload(), StandardCharsets.UTF_8);

      // Ex.: Echo the request back to the client the string "Received <payload>". It needs to be serialized.
      byte[] payload = ("Received " + requestText).getBytes(StandardCharsets.UTF_8);
      transport.send(inbound.connectionId(), payload, sender);

    } catch (RuntimeException exception) {
      System.out.printf("Client request error from %s: %s%n", senderText, exception.getMessage());

    } finally {
      try {
        transport.closeConnection(inbound.connectionId(), sender);
      } catch (RuntimeException closeError) {
        System.out.printf("Client close error from %s: %s%n", senderText, closeError.getMessage());
      }
    }
  }

  private static void handleNodeRequest(AuthenticatedLink transport, Dpch inbound, InetSocketAddress sender) {
    // TODO: consensus logic goes here.
  }
}
