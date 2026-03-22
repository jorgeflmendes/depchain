package pt.ulisboa.depchain.client;

import java.net.InetSocketAddress;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.ClientResponse;
import pt.ulisboa.depchain.proto.QueryRequest;
import pt.ulisboa.depchain.proto.QueryResponse;
import pt.ulisboa.depchain.proto.QueryType;
import pt.ulisboa.depchain.proto.TransactionRequest;
import pt.ulisboa.depchain.proto.TransactionResponse;
import pt.ulisboa.depchain.proto.TransactionType;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.keys.PrivateKeyLoader;
import pt.ulisboa.depchain.shared.keys.PublicKeyLoader;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.ClientRequestSignaturePayloadUtil;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;
import pt.ulisboa.depchain.shared.utils.ProtoValidationUtil;
import pt.ulisboa.depchain.shared.utils.QuorumAccumulator;
import pt.ulisboa.depchain.shared.utils.TimeUtil;

public final class DpchClient implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(DpchClient.class);
  private static final SecureRandom connectionIdRandom = new SecureRandom();
  private static final String INCOHERENT_REPLICA_RESPONSE_MESSAGE = "could not obtain a majority of identical replies from replicas";

  private final List<InetSocketAddress> replicaEndpoints;
  private final PrivateKey clientPrivateKey;
  private final Map<Long, PublicKey> replicaPublicKeys;
  private final long requestTimeoutMs;
  private final int coherentReplyQuorum;
  private final long clientSenderId;
  private final AuthenticatedLink transport;
  private final Map<Long, InetSocketAddress> openConnections;
  private final AtomicLong nextRequestId;

  public DpchClient(String configPath) throws Exception {
    ConfigParser config = ConfigParser.load(java.nio.file.Path.of(configPath));
    this.replicaEndpoints = config.replicas().stream().map(replica -> new InetSocketAddress(replica.host(), replica.clientPort())).toList();
    this.clientSenderId = config.client().senderId();
    this.clientPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config);
    this.replicaPublicKeys = PublicKeyLoader.loadReplicaPublicKeys(config);
    this.requestTimeoutMs = config.client().requestTimeoutMs();
    this.coherentReplyQuorum = config.system().f() + 1;
    this.transport = AuthenticatedLink.unbound(this.clientSenderId, this.clientPrivateKey, this.replicaPublicKeys);
    this.openConnections = new LinkedHashMap<>();
    this.nextRequestId = new AtomicLong(0L);
  }

  public static DpchClient open(String configPath) throws Exception {
    return new DpchClient(configPath);
  }

  public TransactionResponse requestDepCoinTransfer(String recipientAddress, long amount, long nonce, long gasLimit, long gasPrice) throws Exception {
    return requestTransaction(TransactionType.TRANSACTION_TYPE_TRANSFER, recipientAddress, amount, nonce, gasLimit, gasPrice);
  }

  public QueryResponse requestDepCoinBalance(String ownerAddress) throws Exception {
    return requestQuery(QueryType.QUERY_TYPE_DEPCOIN_BALANCE, ownerAddress);
  }

  public QueryResponse requestIstCoinBalance(String ownerAddress) throws Exception {
    return requestQuery(QueryType.QUERY_TYPE_IST_COIN_BALANCE, ownerAddress);
  }

  public TransactionResponse requestIstCoinTransfer(String recipientAddress, long amount, long nonce, long gasLimit, long gasPrice) throws Exception {
    return requestTransaction(TransactionType.TRANSACTION_TYPE_IST_COIN_TRANSFER, recipientAddress, amount, nonce, gasLimit, gasPrice);
  }

  private TransactionResponse requestTransaction(TransactionType type, String to, long amount, long nonce, long gasLimit, long gasPrice) throws Exception {
    ClientResponse response = broadcastAndCollectResponse(buildTransactionRequest(type, to, amount, nonce, gasLimit, gasPrice));
    if (response == null) {
      return null;
    }
    if (!response.hasTransaction()) {
      throw new IllegalArgumentException("Expected transaction response but received " + response.getBodyCase());
    }
    return response.getTransaction();
  }

  private QueryResponse requestQuery(QueryType type, String ownerAddress) throws Exception {
    ClientResponse response = broadcastAndCollectResponse(buildQueryRequest(type, ownerAddress));
    if (response == null) {
      return null;
    }
    if (!response.hasQuery()) {
      throw new IllegalArgumentException("Expected query response but received " + response.getBodyCase());
    }
    return response.getQuery();
  }

  private ClientResponse broadcastAndCollectResponse(ClientRequest request) throws Exception {
    broadcastToReplicas(ProtoValidationUtil.requireValid(request, "ClientRequest").toByteArray());
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

  private void broadcastToReplicas(byte[] payload) {
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

  private ClientRequest buildTransactionRequest(TransactionType type, String to, long amount, long nonce, long gasLimit, long gasPrice) throws Exception {
    long requestId = nextRequestId.getAndIncrement();
    byte[] signature = CryptoUtil
        .signEcdsa(ClientRequestSignaturePayloadUtil.signedTransactionRequestPayload(clientSenderId, requestId, type, to, amount, nonce, gasLimit, gasPrice), clientPrivateKey);

    TransactionRequest.Builder transaction = TransactionRequest.newBuilder().setRequestKey(ClientRequestKey.newBuilder().setClientSenderId(clientSenderId).setRequestId(requestId))
        .setType(type).setTo(to).setAmount(amount).setNonce(nonce).setGasLimit(gasLimit).setGasPrice(gasPrice).setSignature(ByteString.copyFrom(signature));
    return ClientRequest.newBuilder().setTransaction(transaction).build();
  }

  private ClientRequest buildQueryRequest(QueryType type, String owner) throws Exception {
    long requestId = nextRequestId.getAndIncrement();
    byte[] signature = CryptoUtil.signEcdsa(ClientRequestSignaturePayloadUtil.signedQueryRequestPayload(clientSenderId, requestId, type, owner), clientPrivateKey);

    QueryRequest.Builder query = QueryRequest.newBuilder().setRequestKey(ClientRequestKey.newBuilder().setClientSenderId(clientSenderId).setRequestId(requestId)).setType(type)
        .setOwner(owner).setSignature(ByteString.copyFrom(signature));
    return ClientRequest.newBuilder().setQuery(query).build();
  }

  private ClientResponse awaitCoherentResponse(long deadlineNanos) throws IncoherentReplicaResponseException {
    QuorumAccumulator<Long, String, ClientResponse> repliesByValue = new QuorumAccumulator<>(replicaPublicKeys.keySet());

    while (true) {
      InboundPacket inbound = receiveInboundPacket(deadlineNanos);

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

      ClientResponse response = parseResponse(inbound.payload().toByteArray());
      ClientResponse quorumResponse = repliesByValue.recordAndGetFirstValueIfQuorumReached(replicaSenderId, Base64.getEncoder()
          .encodeToString(ProtoValidationUtil.requireValid(response, "ClientResponse").toByteArray()), response, coherentReplyQuorum);

      if (quorumResponse != null) {
        return quorumResponse;
      }

      if (replyQuorumIsImpossible(repliesByValue)) {
        throw new IncoherentReplicaResponseException(INCOHERENT_REPLICA_RESPONSE_MESSAGE);
      }
    }
  }

  private InboundPacket receiveInboundPacket(long deadlineNanos) {
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

  private ClientResponse parseResponse(byte[] payload) {
    try {
      return ProtoValidationUtil.requireValid(ClientResponse.parseFrom(payload), "ClientResponse");
    } catch (InvalidProtocolBufferException exception) {
      throw new IllegalArgumentException("Invalid protobuf client response payload", exception);
    }
  }

  private boolean replyQuorumIsImpossible(QuorumAccumulator<Long, String, ClientResponse> repliesByValue) {
    int remainingPossibleReplies = openConnections.size() - repliesByValue.acceptedCount();
    return repliesByValue.maxCount() + remainingPossibleReplies < coherentReplyQuorum;
  }

  @Override
  public void close() throws Exception {
    transport.close();
  }
}
