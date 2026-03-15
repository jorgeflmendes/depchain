package pt.ulisboa.depchain.server;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.protobuf.InvalidProtocolBufferException;

import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.DpchPacket;
import pt.ulisboa.depchain.proto.Message;
import pt.ulisboa.depchain.server.consensus.Replica;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.keys.PrivateKeyLoader;
import pt.ulisboa.depchain.shared.keys.PublicKeyLoader;
import pt.ulisboa.depchain.shared.keys.ThresholdKeyLoader;
import pt.ulisboa.depchain.shared.logging.Logger;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.ProtoValidationUtil;

public final class DpchServer {
  private static final Logger logger = new Logger("DpchServer");

  private final ConfigParser configParser;
  private final ConfigParser.ReplicaSection replicaConfig;
  private final PrivateKey localStaticSKey;
  private final Map<Long, PublicKey> staticPKeys;
  private final Replica replica;

  public DpchServer(String serverId, String configPath) throws Exception {
    this.configParser = ConfigParser.load(Path.of(configPath));
    this.replicaConfig = configParser.requireReplicaById(serverId);
    this.localStaticSKey = PrivateKeyLoader.loadReplicaPrivateKey(configParser, replicaConfig.senderId());
    this.staticPKeys = PublicKeyLoader.loadStaticPublicKeys(configParser);
    ThresholdKeyLoader.ReplicaThresholdKeyMaterial thresholdKeys = ThresholdKeyLoader.loadReplicaThresholdKeyMaterial(configParser, replicaConfig.senderId());
    PublicKey clientPublicKey = staticPKeys.get(configParser.client().senderId());
    this.replica = new Replica(Math.toIntExact(replicaConfig.senderId()), configParser, thresholdKeys.privateShare(), thresholdKeys.publicKey(), clientPublicKey);
  }

  public void run() throws Exception {
    InetSocketAddress clientBindEndpoint = new InetSocketAddress(InetAddress.getByName(replicaConfig.host()), replicaConfig.clientPort());
    InetSocketAddress nodeBindEndpoint = new InetSocketAddress(InetAddress.getByName(replicaConfig.host()), replicaConfig.consensusPort());
    ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();

    try (workers;
        AuthenticatedLink clientTransport = AuthenticatedLink.bind(clientBindEndpoint, replicaConfig.senderId(), localStaticSKey, staticPKeys);
        AuthenticatedLink nodeTransport = AuthenticatedLink.bind(nodeBindEndpoint, replicaConfig.senderId(), localStaticSKey, staticPKeys)) {

      logger.info("Replica " + replicaConfig.id() + " client listener: " + replicaConfig.host() + ":" + replicaConfig.clientPort());
      logger.info("Replica " + replicaConfig.id() + " node listener: " + replicaConfig.host() + ":" + replicaConfig.consensusPort());

      // Set up the replica with the transports and start the hotstuff loop
      this.replica.initNetwork(nodeTransport, clientTransport);
      workers.submit(() -> this.replica.run());

      // New thread for handling messages from other replicas
      workers.submit(() -> runNodeLoop(nodeTransport, workers));

      // This thread will handle client requests
      runClientLoop(clientTransport, workers);
    }
  }

  // Loop for handling messages from clients
  private void runClientLoop(AuthenticatedLink transport, ExecutorService workers) {
    while (true) {
      InboundPacket request = receiveNextInbound(transport);
      if (request == null) {
        continue;
      }

      workers.submit(() -> handleClientRequest(transport, request.packet(), request.sender()));
    }
  }

  // Loop for handling messages from other replicas
  private void runNodeLoop(AuthenticatedLink transport, ExecutorService workers) {
    while (true) {
      InboundPacket request = receiveNextInbound(transport);
      if (request == null) {
        continue;
      }

      workers.submit(() -> handleNodeRequest(transport, request.packet(), request.sender()));
    }
  }

  // Receives the next inbound packet
  private InboundPacket receiveNextInbound(AuthenticatedLink transport) {
    try {
      return transport.receive();
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  // Handles messages from clients
  private void handleClientRequest(AuthenticatedLink transport, DpchPacket inbound, InetSocketAddress sender) {
    String senderText = sender.getAddress().getHostAddress() + ":" + sender.getPort();
    logger.info("Client request from " + sender);

    try {
      ClientRequest request = ProtoValidationUtil.requireValid(ClientRequest.parseFrom(inbound.getPayload()), "ClientRequest");
      ConnectionKey key = new ConnectionKey(sender, inbound.getConnectionId());
      this.replica.receiveClientCommand(request, key);
    } catch (InvalidProtocolBufferException | RuntimeException exception) {
      logger.error("Client request error from " + senderText + ": " + exception.getMessage());
    }
  }

  // Handles messages from other replicas
  private void handleNodeRequest(AuthenticatedLink transport, DpchPacket inbound, InetSocketAddress sender) {
    try {
      Message msg = ProtoValidationUtil.requireValid(Message.parseFrom(inbound.getPayload()), "ReplicaMessage");
      this.replica.receiveMessage(msg);
    } catch (InvalidProtocolBufferException | RuntimeException e) {
      logger.error("Error handling node message from " + sender + ": " + e.getMessage());
    }
  }
}
