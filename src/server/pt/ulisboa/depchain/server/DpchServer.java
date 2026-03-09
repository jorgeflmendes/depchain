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

import com.weavechain.curve25519.Scalar;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.keys.PrivateKeyLoader;
import pt.ulisboa.depchain.shared.keys.PublicKeyLoader;
import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.SerializationUtil;

public final class DpchServer {
  private final ConfigParser configParser;
  private final ConfigParser.ReplicaSection replicaConfig;
  private final PrivateKey localStaticSKey;
  private final Map<Long, PublicKey> staticPKeys;
  private final Replica replica;

  public DpchServer(String serverId, String configPath) throws Exception {
    this.configParser = ConfigParser.load(Path.of(configPath));
    this.replicaConfig = configParser.requireReplica(serverId);
    byte[] thresholdPublicKey = PublicKeyLoader.loadReplicaThresholdPublicKey(configParser, replicaConfig.senderId());
    Scalar share = PrivateKeyLoader.loadReplicaThresholdPrivateShare(configParser, replicaConfig.senderId());
    this.replica = new Replica(Math.toIntExact(replicaConfig.senderId()), configParser, share, thresholdPublicKey);
    this.localStaticSKey = PrivateKeyLoader.loadReplicaPrivateKey(configParser, replicaConfig.senderId());
    this.staticPKeys = PublicKeyLoader.loadStaticPublicKeys(configParser);

  }

  public void run() throws Exception {
    InetSocketAddress clientBindEndpoint = new InetSocketAddress(InetAddress.getByName(replicaConfig.host()), replicaConfig.clientPort());
    InetSocketAddress nodeBindEndpoint = new InetSocketAddress(InetAddress.getByName(replicaConfig.host()), replicaConfig.consensusPort());
    ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();

    try (workers;
        AuthenticatedLink clientTransport = AuthenticatedLink.bind(clientBindEndpoint, replicaConfig.senderId(), localStaticSKey, staticPKeys);
        AuthenticatedLink nodeTransport = AuthenticatedLink.bind(nodeBindEndpoint, replicaConfig.senderId(), localStaticSKey, staticPKeys)) {

      System.out.printf("Replica %s client listener: %s:%d%n", replicaConfig.id(), replicaConfig.host(), replicaConfig.clientPort());
      System.out.printf("Replica %s node listener: %s:%d%n", replicaConfig.id(), replicaConfig.host(), replicaConfig.consensusPort());

      this.replica.initNetwork(nodeTransport, clientTransport);

      workers.submit(() -> this.replica.run());
      workers.submit(() -> runNodeLoop(nodeTransport, workers));
      runClientLoop(clientTransport, workers);
    }
  }

  private void runClientLoop(AuthenticatedLink transport, ExecutorService workers) {
    while (true) {
      InboundPacket request = receiveNextInbound(transport);
      if (request == null) {
        continue;
      }

      workers.submit(() -> handleClientRequest(transport, request.packet(), request.sender()));
    }
  }

  private void runNodeLoop(AuthenticatedLink transport, ExecutorService workers) {
    while (true) {
      InboundPacket request = receiveNextInbound(transport);
      if (request == null) {
        continue;
      }

      workers.submit(() -> handleNodeRequest(transport, request.packet(), request.sender()));
    }
  }

  private InboundPacket receiveNextInbound(AuthenticatedLink transport) {
    try {
      return transport.receive();
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  private void handleClientRequest(AuthenticatedLink transport, Dpch inbound, InetSocketAddress sender) {
    String senderText = sender.getAddress().getHostAddress() + ":" + sender.getPort();
    System.out.printf("Client request from %s%n", sender);

    try {
      String requestText = new String(inbound.payload(), StandardCharsets.UTF_8);
      ConnectionKey key = new ConnectionKey(sender, inbound.connectionId());
      this.replica.receiveCommand(requestText, key);
      // TODO: Optionally, send to the client "Request received and being processed";

    } catch (RuntimeException exception) {
      System.out.printf("Client request error from %s: %s%n", senderText, exception.getMessage());
    }
  }

  private void handleNodeRequest(AuthenticatedLink transport, Dpch inbound, InetSocketAddress sender) {
    try {
      Message msg = SerializationUtil.decodeMessage(inbound.payload());
      this.replica.receiveMessage(msg);
    } catch (Exception e) {
      System.err.printf("Error handling node message from %s: %s%n", sender, e.getMessage());
    }
  }
}
