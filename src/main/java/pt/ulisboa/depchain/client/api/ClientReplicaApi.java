package pt.ulisboa.depchain.client.api;

import java.net.InetSocketAddress;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import pt.ulisboa.depchain.client.exception.IncoherentReplicaResponseException;
import pt.ulisboa.depchain.client.request.SignedClientRequestFactory;
import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientResponse;
import pt.ulisboa.depchain.proto.QueryResponse;
import pt.ulisboa.depchain.proto.QueryType;
import pt.ulisboa.depchain.proto.TransactionResponse;
import pt.ulisboa.depchain.proto.TransactionType;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.crypto.CryptoUtil;
import pt.ulisboa.depchain.shared.crypto.key.PrivateKeyLoader;
import pt.ulisboa.depchain.shared.crypto.key.PublicKeyLoader;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.quorum.QuorumAccumulator;
import pt.ulisboa.depchain.shared.time.TimeUtil;
import pt.ulisboa.depchain.shared.validation.ProtoValidationUtil;
import pt.ulisboa.depchain.shared.validation.ValidationUtils;

public final class ClientReplicaApi implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(ClientReplicaApi.class);
  private static final SecureRandom connectionIdRandom = new SecureRandom();
  private static final String INCOHERENT_REPLICA_RESPONSE_MESSAGE = "could not obtain a majority of identical replies from replicas";

  private final List<InetSocketAddress> replicaEndpoints;
  private final Map<Long, PublicKey> replicaPublicKeys;
  private final long requestTimeoutMs;
  private final int coherentReplyQuorum;
  private final String walletAddress;
  private final AuthenticatedLink transport;
  private final Map<Long, InetSocketAddress> openConnections;
  private final SignedClientRequestFactory requestFactory;

  public ClientReplicaApi(String configPath, String clientId) throws Exception {
    ValidationUtils.requireNonBlank(configPath, "configPath");
    ValidationUtils.requireNonBlank(clientId, "clientId");

    ConfigParser config = ConfigParser.load(java.nio.file.Path.of(configPath));
    ConfigParser.ClientSection selectedClient = config.requireClientById(clientId);

    this.replicaEndpoints = selectedClient.knownReplicas().stream().map(config::requireReplicaById).map(replica -> new InetSocketAddress(replica.host(), replica.clientPort()))
        .toList();
    long clientSenderId = selectedClient.senderId();
    PrivateKey clientPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config, clientSenderId);
    PublicKey clientPublicKey = PublicKeyLoader.loadClientPublicKey(config, clientSenderId);
    this.replicaPublicKeys = PublicKeyLoader.loadReplicaPublicKeys(config);
    this.requestTimeoutMs = selectedClient.requestTimeoutMs();
    this.coherentReplyQuorum = config.system().f() + 1;
    this.walletAddress = CryptoUtil.deriveAddressHex(clientPublicKey);
    this.transport = AuthenticatedLink.unbound(clientSenderId, clientPrivateKey, this.replicaPublicKeys);
    this.openConnections = new LinkedHashMap<>();
    this.requestFactory = new SignedClientRequestFactory(clientSenderId, clientPrivateKey);
  }

  public static ClientReplicaApi connect(String configPath, String clientId) throws Exception {
    return new ClientReplicaApi(configPath, clientId);
  }

  public String getWalletAddress() {
    return walletAddress;
  }

  public TransactionResponse transferDepCoin(String recipientAddress, long amount, long nonce, long gasLimit, long gasPrice) throws Exception {
    return submitTransactionRequest(TransactionType.TRANSACTION_TYPE_TRANSFER, recipientAddress, amount, nonce, gasLimit, gasPrice);
  }

  public QueryResponse getDepCoinBalance(String ownerAddress) throws Exception {
    return submitQueryRequest(QueryType.QUERY_TYPE_DEPCOIN_BALANCE, ownerAddress);
  }

  public QueryResponse getOwnDepCoinBalance() throws Exception {
    return getDepCoinBalance(walletAddress);
  }

  public QueryResponse getIstCoinBalance(String ownerAddress) throws Exception {
    return submitQueryRequest(QueryType.QUERY_TYPE_IST_COIN_BALANCE, ownerAddress);
  }

  public QueryResponse getOwnIstCoinBalance() throws Exception {
    return getIstCoinBalance(walletAddress);
  }

  public TransactionResponse transferIstCoin(String recipientAddress, long amount, long nonce, long gasLimit, long gasPrice) throws Exception {
    return submitTransactionRequest(TransactionType.TRANSACTION_TYPE_IST_COIN_TRANSFER, recipientAddress, amount, nonce, gasLimit, gasPrice);
  }

  public TransactionResponse callContract(String contractAddress, long amount, long nonce, long gasLimit, long gasPrice, byte[] input) throws Exception {
    return submitTransactionRequest(TransactionType.TRANSACTION_TYPE_CONTRACT_CALL, contractAddress, amount, nonce, gasLimit, gasPrice, input);
  }

  private TransactionResponse submitTransactionRequest(TransactionType type, String to, long amount, long nonce, long gasLimit, long gasPrice) throws Exception {
    return submitTransactionRequest(type, to, amount, nonce, gasLimit, gasPrice, new byte[0]);
  }

  private TransactionResponse submitTransactionRequest(TransactionType type, String to, long amount, long nonce, long gasLimit, long gasPrice, byte[] input) throws Exception {
    ClientResponse response = sendRequestAndCollectResponse(requestFactory.createTransactionRequest(type, to, amount, nonce, gasLimit, gasPrice, input));
    if (response == null) {
      return null;
    }
    if (!response.hasTransaction()) {
      throw new IllegalArgumentException("Expected transaction response but received " + response.getBodyCase());
    }
    return response.getTransaction();
  }

  private QueryResponse submitQueryRequest(QueryType type, String ownerAddress) throws Exception {
    ClientResponse response = sendRequestAndCollectResponse(requestFactory.createQueryRequest(type, ownerAddress));
    if (response == null) {
      return null;
    }
    if (!response.hasQuery()) {
      throw new IllegalArgumentException("Expected query response but received " + response.getBodyCase());
    }
    return response.getQuery();
  }

  private ClientResponse sendRequestAndCollectResponse(ClientRequest request) throws Exception {
    sendRequestToReplicas(ProtoValidationUtil.requireValid(request, "ClientRequest").toByteArray());
    if (openConnections.isEmpty()) {
      return null;
    }

    long deadlineNanos = requestTimeoutMs == 0L ? Long.MAX_VALUE : TimeUtil.monotonicDeadlineAfterNow(requestTimeoutMs);

    try {
      return awaitCoherentResponse(deadlineNanos);
    } finally {
      closeOpenConnections();
    }
  }

  private void sendRequestToReplicas(byte[] payload) {
    openConnections.clear();
    for (InetSocketAddress replicaEndpoint : replicaEndpoints) {
      long connectionId = nextConnectionId();

      try {
        transport.send(connectionId, payload, replicaEndpoint);
        openConnections.put(connectionId, replicaEndpoint);
      } catch (RuntimeException exception) {
        logger.debug("Ignoring failed client broadcast init to {}", replicaEndpoint, exception);
      }
    }
  }

  private long nextConnectionId() {
    while (true) {
      long connectionId = connectionIdRandom.nextLong();
      if (connectionId != 0L && !openConnections.containsKey(connectionId)) {
        return connectionId;
      }
    }
  }

  private void closeOpenConnections() {
    for (Map.Entry<Long, InetSocketAddress> entry : openConnections.entrySet()) {
      try {
        transport.closeConnection(entry.getKey(), entry.getValue());
      } catch (RuntimeException exception) {
        logger.debug("Ignoring client connection close failure", exception);
      }
    }
    openConnections.clear();
  }

  private ClientResponse awaitCoherentResponse(long deadlineNanos) throws IncoherentReplicaResponseException {
    QuorumAccumulator<Long, String, ClientResponse> repliesByValue = new QuorumAccumulator<>(replicaPublicKeys.keySet());

    while (true) {
      InboundPacket inbound = receiveResponsePacket(deadlineNanos);

      if (inbound == null) {
        if (repliesByValue.acceptedCount() > 0) {
          throw new IncoherentReplicaResponseException(INCOHERENT_REPLICA_RESPONSE_MESSAGE);
        }
        return null;
      }

      if (!openConnections.containsKey(inbound.packet().getConnectionId())) {
        continue;
      }

      Long replicaSenderId = inbound.authenticatedSenderId();
      if (replicaSenderId == null) {
        continue;
      }

      ClientResponse response = parseClientResponse(inbound.payload().toByteArray());
      ClientResponse quorumResponse = repliesByValue.recordAndGetFirstValueIfQuorumReached(replicaSenderId, Base64.getEncoder()
          .encodeToString(ProtoValidationUtil.requireValid(response, "ClientResponse").toByteArray()), response, coherentReplyQuorum);

      if (quorumResponse != null) {
        return quorumResponse;
      }

      if (isReplyQuorumImpossible(repliesByValue)) {
        throw new IncoherentReplicaResponseException(INCOHERENT_REPLICA_RESPONSE_MESSAGE);
      }
    }
  }

  private InboundPacket receiveResponsePacket(long deadlineNanos) {
    while (true) {
      try {
        if (requestTimeoutMs == 0L) {
          InboundPacket inbound;
          do {
            inbound = transport.receive();
          } while (inbound == null);
          return inbound;
        }

        InboundPacket inbound = null;
        while (!TimeUtil.hasTimedOutMonotonic(deadlineNanos) && inbound == null) {
          inbound = transport.receive(TimeUtil.monotonicRemainingMsUntil(deadlineNanos));
        }
        return inbound;
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        return null;
      } catch (Exception exception) {
        logger.debug("Ignoring client receive failure", exception);
      }
    }
  }

  private ClientResponse parseClientResponse(byte[] payload) {
    try {
      return ProtoValidationUtil.requireValid(ClientResponse.parseFrom(payload), "ClientResponse");
    } catch (InvalidProtocolBufferException exception) {
      throw new IllegalArgumentException("Invalid protobuf client response payload", exception);
    }
  }

  private boolean isReplyQuorumImpossible(QuorumAccumulator<Long, String, ClientResponse> repliesByValue) {
    int remainingPossibleReplies = openConnections.size() - repliesByValue.acceptedCount();
    return repliesByValue.maxCount() + remainingPossibleReplies < coherentReplyQuorum;
  }

  @Override
  public void close() throws Exception {
    transport.close();
  }
}
