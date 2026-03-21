package pt.ulisboa.depchain.server.runtime;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.DpchPacket;
import pt.ulisboa.depchain.proto.Message;
import pt.ulisboa.depchain.server.consensus.hotstuff.HotStuffManager;
import pt.ulisboa.depchain.server.evm.EvmService;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.config.GenesisParser;
import pt.ulisboa.depchain.shared.keys.PrivateKeyLoader;
import pt.ulisboa.depchain.shared.keys.PublicKeyLoader;
import pt.ulisboa.depchain.shared.keys.ThresholdKeyLoader;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.ProtoValidationUtil;

public final class ReplicaServer {
  private static final Logger logger = LoggerFactory.getLogger(ReplicaServer.class);

  private final ConfigParser configParser;
  private final ConfigParser.ReplicaSection replicaConfig;
  private final PrivateKey localStaticSKey;
  private final Map<Long, PublicKey> clientStaticPKeys;
  private final Map<Long, PublicKey> replicaStaticPKeys;
  private final Map<String, Long> replicaSenderIdByConsensusEndpoint;
  private final GenesisParser genesis;
  private final EvmService evmService;
  private final HotStuffManager hotStuffManager;

  public ReplicaServer(String serverId, String configPath) throws Exception {
    this.configParser = ConfigParser.load(Path.of(configPath));
    this.replicaConfig = configParser.requireReplicaById(serverId);
    this.localStaticSKey = PrivateKeyLoader.loadReplicaPrivateKey(configParser, replicaConfig.senderId());
    this.clientStaticPKeys = Map.of(configParser.client().senderId(), PublicKeyLoader.loadClientPublicKey(configParser));
    this.replicaStaticPKeys = PublicKeyLoader.loadReplicaPublicKeys(configParser);
    this.replicaSenderIdByConsensusEndpoint = buildReplicaSenderIdByConsensusEndpoint(configParser);
    this.genesis = GenesisParser.loadDefaultResource();
    this.evmService = new EvmService();
    applyGenesisState(evmService, genesis);
    applyGenesisTransactions(evmService, genesis);
    ThresholdKeyLoader.ReplicaThresholdKeyMaterial thresholdKeys = ThresholdKeyLoader.loadReplicaThresholdKeyMaterial(configParser, replicaConfig.senderId());
    PublicKey clientPublicKey = clientStaticPKeys.get(configParser.client().senderId());
    this.hotStuffManager = new HotStuffManager(Math.toIntExact(replicaConfig.senderId()), configParser, thresholdKeys.privateShare(), thresholdKeys.publicKey(), clientPublicKey,
      evmService);
  }

  public void run() throws Exception {
    InetSocketAddress clientBindEndpoint = new InetSocketAddress(InetAddress.getByName(replicaConfig.host()), replicaConfig.clientPort());
    InetSocketAddress nodeBindEndpoint = new InetSocketAddress(InetAddress.getByName(replicaConfig.host()), replicaConfig.consensusPort());
    ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();

    try (workers;
        AuthenticatedLink clientTransport = AuthenticatedLink.bind(clientBindEndpoint, replicaConfig.senderId(), localStaticSKey, clientStaticPKeys);
        AuthenticatedLink nodeTransport = AuthenticatedLink.bind(nodeBindEndpoint, replicaConfig.senderId(), localStaticSKey, replicaStaticPKeys)) {

      logger.info("Replica {} client listener: {}:{}", replicaConfig.id(), replicaConfig.host(), replicaConfig.clientPort());
      logger.info("Replica {} node listener: {}:{}", replicaConfig.id(), replicaConfig.host(), replicaConfig.consensusPort());
      logger.info("Loaded genesis block height={} hash={} txs={} accounts={}", genesis.height(), shortHash(genesis.blockHash()), genesis.transactions().size(), genesis.state().size());

      // Set up the replica with the transports and start the hotstuff loop
      this.hotStuffManager.initNetwork(nodeTransport, clientTransport);
      workers.submit(() -> this.hotStuffManager.run());

      // Dedicated loop for inter-replica traffic.
      workers.submit(() -> runNodeLoop(nodeTransport));

      // This thread handles client traffic inline to avoid per-packet task churn.
      runClientLoop(clientTransport);
    }
  }

  // Loop for handling messages from clients
  private void runClientLoop(AuthenticatedLink transport) {
    while (true) {
      InboundPacket request = receiveNextInbound(transport);
      if (request == null) {
        continue;
      }

      handleClientRequest(request);
    }
  }

  // Loop for handling messages from other replicas
  private void runNodeLoop(AuthenticatedLink transport) {
    while (true) {
      InboundPacket request = receiveNextInbound(transport);
      if (request == null) {
        continue;
      }

      handleNodeRequest(request);
    }
  }

  // Receives the next inbound packet
  private InboundPacket receiveNextInbound(AuthenticatedLink transport) {
    try {
      return transport.receive();
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      return null;
    } catch (Exception exception) {
      logger.debug("Ignoring authenticated receive failure", exception);
      return null;
    }
  }

  // Handles messages from clients
  private void handleClientRequest(InboundPacket inbound) {
    DpchPacket packet = inbound.packet();
    InetSocketAddress sender = inbound.sender();
    String senderText = sender.getAddress().getHostAddress() + ":" + sender.getPort();
    logger.debug("Client request from {}", sender);

    try {
      if (inbound.authenticatedSenderId() == null || inbound.authenticatedSenderId() != configParser.client().senderId()) {
        logger.warn("Rejecting client request from {} with unauthenticated senderId {}", sender, inbound.authenticatedSenderId());
        return;
      }

      ClientRequest request = ProtoValidationUtil.requireValid(ClientRequest.parseFrom(inbound.payload()), "ClientRequest");
      ConnectionKey key = new ConnectionKey(sender, packet.getConnectionId());
      this.hotStuffManager.onClientRequest(request, key);
    } catch (InvalidProtocolBufferException | RuntimeException exception) {
      logger.error("Client request error from {}", senderText, exception);
    }
  }

  // Handles messages from other replicas
  private void handleNodeRequest(InboundPacket inbound) {
    DpchPacket packet = inbound.packet();
    InetSocketAddress sender = inbound.sender();
    try {
      Long expectedSenderId = replicaSenderIdByConsensusEndpoint.get(endpointKey(sender));
      if (expectedSenderId == null) {
        logger.warn("Rejecting node message from unknown consensus endpoint {}", sender);
        return;
      }

      if (inbound.authenticatedSenderId() == null || !expectedSenderId.equals(inbound.authenticatedSenderId())) {
        logger.warn("Rejecting node message from {} with unauthenticated senderId {}", sender, inbound.authenticatedSenderId());
        return;
      }

      Message msg = ProtoValidationUtil.requireValid(Message.parseFrom(inbound.payload()), "ReplicaMessage");
      if (msg.getReplicaSenderId() != expectedSenderId.intValue()) {
        logger.warn("Rejecting spoofed node message from {} claiming senderId {}", sender, msg.getReplicaSenderId());
        return;
      }

      this.hotStuffManager.onReplicaMessage(msg);
    } catch (InvalidProtocolBufferException | RuntimeException e) {
      logger.error("Error handling node message from {}", sender, e);
    }
  }

  private static Map<String, Long> buildReplicaSenderIdByConsensusEndpoint(ConfigParser configParser) {
    Map<String, Long> senderIdByEndpoint = new HashMap<>();
    for (ConfigParser.ReplicaSection replica : configParser.replicas()) {
      senderIdByEndpoint.put(endpointKey(replica.host(), replica.consensusPort()), replica.senderId());
    }
    return Map.copyOf(senderIdByEndpoint);
  }

  private static String endpointKey(InetSocketAddress endpoint) {
    return endpointKey(endpoint.getAddress().getHostAddress(), endpoint.getPort());
  }

  private static String endpointKey(String host, int port) {
    return host + ":" + port;
  }

  private static String shortHash(String hash) {
    if (hash == null || hash.length() < 12) {
      return hash;
    }

    return hash.substring(0, 12);
  }

  private static void applyGenesisState(EvmService evmService, GenesisParser genesis) {
    for (Map.Entry<String, GenesisParser.GenesisAccount> entry : genesis.state().entrySet()) {
      String addressHex = entry.getKey();
      GenesisParser.GenesisAccount genesisAccount = entry.getValue();
      Address address = parseAddress(addressHex, "state address");
      Wei balance = Wei.of(new BigInteger(genesisAccount.balance()));
      var account = evmService.createAccount(address, genesisAccount.nonce(), balance);

      if (genesisAccount.code() != null && !genesisAccount.code().isBlank()) {
        account.setCode(Bytes.fromHexString(genesisAccount.code()));
      }
    }
  }

  private static void applyGenesisTransactions(EvmService evmService, GenesisParser genesis) {
    for (int i = 0; i < genesis.transactions().size(); i++) {
      GenesisParser.GenesisTransaction tx = genesis.transactions().get(i);
      try {
        applyGenesisTransaction(evmService, tx);
      } catch (RuntimeException exception) {
        throw new IllegalArgumentException("Failed to execute genesis transaction at index " + i + ": " + exception.getMessage(), exception);
      }
    }
  }

  private static void applyGenesisTransaction(EvmService evmService, GenesisParser.GenesisTransaction tx) {
    Address from = parseAddress(tx.from(), "transaction.from");
    Wei amount = Wei.of(new BigInteger(tx.amount()));
    Wei gasPrice = Wei.of(tx.gasPrice());

    switch (tx.type()) {
      case "TRANSFER": {
        Address to = parseAddress(tx.to(), "transaction.to");
        EvmService.TransactionResult result = evmService.transferNative(from, to, amount, tx.nonce(), tx.gasLimit(), gasPrice);
        if (!result.success()) {
          throw new IllegalArgumentException(result.errorMessage());
        }
        break;
      }
      case "CONTRACT_CALL": {
        Address to = parseAddress(tx.to(), "transaction.to");
        EvmService.TransactionResult result = evmService.callContract(from, to, parseHexBytes(tx.input(), "transaction.input"), amount, tx.nonce(), tx.gasLimit(), gasPrice);
        if (!result.success()) {
          throw new IllegalArgumentException(result.errorMessage());
        }
        break;
      }
      case "CONTRACT_DEPLOY": {
        evmService.deployContract(from, parseHexBytes(tx.input(), "transaction.input"));
        break;
      }
      default:
        throw new IllegalArgumentException("unsupported transaction type: " + tx.type());
    }
  }

  private static Address parseAddress(String addressHex, String fieldName) {
    if (addressHex == null || addressHex.isBlank()) {
      throw new IllegalArgumentException(fieldName + " must be a 40-char hex address");
    }
    return Address.fromHexString("0x" + addressHex);
  }

  private static Bytes parseHexBytes(String value, String fieldName) {
    try {
      return Bytes.fromHexString(value);
    } catch (RuntimeException exception) {
      throw new IllegalArgumentException(fieldName + " is not a valid hex payload", exception);
    }
  }
}
