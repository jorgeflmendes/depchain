package pt.ulisboa.depchain.server.api;

import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.ClientResponse;
import pt.ulisboa.depchain.proto.Node;
import pt.ulisboa.depchain.proto.NodeCommand;
import pt.ulisboa.depchain.proto.QueryRequest;
import pt.ulisboa.depchain.proto.TransactionRequest;
import pt.ulisboa.depchain.server.consensus.ConsensusTimeoutException;
import pt.ulisboa.depchain.server.node.BlockStore;
import pt.ulisboa.depchain.shared.crypto.ClientRequestSignaturePayloadUtil;
import pt.ulisboa.depchain.shared.crypto.CryptoUtil;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.time.TimeUtil;
import pt.ulisboa.depchain.shared.validation.ProtoValidationUtil;
import pt.ulisboa.depchain.shared.validation.ValidationUtils;

public final class ReplicaClientApi {
  private final Map<Long, PublicKey> clientPublicKeys;
  private final Logger logger;
  private final BlockingQueue<QueuedClientRequest> pendingCommands = new PriorityBlockingQueue<>(11, QueuedClientRequest.ORDERING);
  private final Map<ClientRequestKey, ConnectionKey> clientContexts = new ConcurrentHashMap<>();
  private final Map<ClientRequestKey, ClientRequest> pendingRequests = new ConcurrentHashMap<>();
  private final Set<ClientRequestKey> queuedRequestIds = ConcurrentHashMap.newKeySet();
  private final Set<ClientRequestKey> completedRequestIds = ConcurrentHashMap.newKeySet();
  private final AtomicLong enqueueSequence = new AtomicLong();
  private AuthenticatedLink clientTransport;

  public ReplicaClientApi(Map<Long, PublicKey> clientPublicKeys, Logger logger) {
    ValidationUtils.requireNonNull(clientPublicKeys, "clientPublicKeys");
    if (clientPublicKeys.isEmpty()) {
      throw new IllegalArgumentException("clientPublicKeys must not be empty");
    }
    this.clientPublicKeys = Map.copyOf(clientPublicKeys);
    this.logger = ValidationUtils.requireNonNull(logger, "logger");
  }

  public void bindTransport(AuthenticatedLink clientTransport) {
    this.clientTransport = clientTransport;
  }

  public void registerClientRequest(ClientRequest request, ConnectionKey key, boolean isLeader) {
    ClientRequest acceptedRequest = registerKnownRequest(request, key);
    if (acceptedRequest != null && isLeader) {
      enqueueIfNotQueued(acceptedRequest);
    }
  }

  public boolean registerProposedCommand(NodeCommand command) {
    List<ClientRequest> requests = clientRequests(command);
    if (requests.isEmpty()) {
      return true;
    }
    if (!canAcceptAllRequests(requests)) {
      return false;
    }

    for (ClientRequest request : requests) {
      ClientRequestKey requestKey = requestKeyOrNull(request);
      pendingRequests.putIfAbsent(requestKey, request);
    }
    return true;
  }

  public int getPendingRequestCount() {
    return pendingRequests.size();
  }

  public int enqueuePendingRequestsIfLeader(boolean isLeader) {
    if (!isLeader || pendingRequests.isEmpty()) {
      return 0;
    }
    return enqueuePendingRequests();
  }

  public void requeuePendingRequests(List<ClientRequest> requests) {
    ValidationUtils.requireNonNull(requests, "requests");
    for (ClientRequest request : requests) {
      ClientRequestKey requestKey = requestKeyOrNull(request);
      if (requestKey == null || !pendingRequests.containsKey(requestKey) || completedRequestIds.contains(requestKey)) {
        continue;
      }
      enqueueIfNotQueued(request);
    }
  }

  public void discardPendingRequest(ClientRequest request) {
    ClientRequestKey requestKey = requestKeyOrNull(request);
    if (requestKey == null) {
      return;
    }

    pendingRequests.remove(requestKey);
    queuedRequestIds.remove(requestKey);
    clientContexts.remove(requestKey);
  }

  public ClientRequest awaitNextPendingRequest(long deadlineNanos) {
    while (true) {
      QueuedClientRequest nextQueuedRequest = pollPendingCommand(deadlineNanos);
      if (nextQueuedRequest == null) {
        return null;
      }

      ClientRequest nextRequest = nextQueuedRequest.request();
      ClientRequestKey requestKey = requestKeyOrNull(nextRequest);
      queuedRequestIds.remove(requestKey);
      if (!completedRequestIds.contains(requestKey) && pendingRequests.containsKey(requestKey)) {
        return nextRequest;
      }
    }
  }

  public List<ClientRequest> awaitNextPendingBatch(long deadlineNanos, long maxBlockGasLimit, int maxBatchSize) {
    ValidationUtils.requireNonNegativeLong(maxBlockGasLimit, "maxBlockGasLimit");
    ValidationUtils.requirePositiveInt(maxBatchSize, "maxBatchSize");

    List<ClientRequest> batch = new ArrayList<>();
    List<QueuedClientRequest> deferred = new ArrayList<>();
    long totalGasLimit = 0L;

    try {
      while (batch.isEmpty()) {
        QueuedClientRequest firstQueuedRequest = pollPendingCommand(deadlineNanos);
        if (firstQueuedRequest == null) {
          return List.of();
        }

        ClientRequest firstRequest = activeRequestOrNull(firstQueuedRequest);
        if (firstRequest == null) {
          continue;
        }

        selectQueuedRequest(firstRequest);
        batch.add(firstRequest);
        totalGasLimit = gasLimitOf(firstRequest);
      }

      while (batch.size() < maxBatchSize) {
        QueuedClientRequest nextQueuedRequest = pendingCommands.poll();
        if (nextQueuedRequest == null) {
          break;
        }

        ClientRequest nextRequest = activeRequestOrNull(nextQueuedRequest);
        if (nextRequest == null) {
          continue;
        }

        long nextGasLimit = gasLimitOf(nextRequest);
        if (totalGasLimit + nextGasLimit > maxBlockGasLimit) {
          deferred.add(nextQueuedRequest);
          continue;
        }

        selectQueuedRequest(nextRequest);
        batch.add(nextRequest);
        totalGasLimit += nextGasLimit;
      }
    } finally {
      for (QueuedClientRequest deferredRequest : deferred) {
        pendingCommands.offer(deferredRequest);
      }
    }

    return List.copyOf(batch);
  }

  public List<ExecutionContext> recordExecutedNode(Node node) {
    NodeCommand command = node.getCommand();
    List<ClientRequest> requests = clientRequests(command);
    if (requests.isEmpty()) {
      return List.of();
    }

    List<ExecutionContext> results = new ArrayList<>(requests.size());
    for (ClientRequest request : requests) {
      ClientRequestKey requestKey = requestKeyOrNull(request);
      if (requestKey == null) {
        continue;
      }

      completedRequestIds.add(requestKey);
      pendingRequests.remove(requestKey);
      queuedRequestIds.remove(requestKey);
      results.add(new ExecutionContext(request, clientContexts.remove(requestKey)));
    }
    return List.copyOf(results);
  }

  public void sendClientResponse(ConnectionKey key, ClientResponse response) {
    if (clientTransport == null || key == null) {
      return;
    }

    try {
      byte[] payload = ProtoValidationUtil.requireValid(response, "ClientResponse").toByteArray();
      clientTransport.send(key.connectionId(), payload, key.endpoint());
    } catch (Exception exception) {
      logger.error("Error replying to client connection", exception);
    }
  }

  public boolean isValidClientRequest(ClientRequest request) {
    return fitsBlockGasPolicy(request) && isValidClientRequestSignature(request);
  }

  private ClientRequest registerKnownRequest(ClientRequest request, ConnectionKey key) {
    if (!canAcceptAllRequests(List.of(request))) {
      return null;
    }

    ClientRequestKey requestKey = requestKeyOrNull(request);
    if (completedRequestIds.contains(requestKey)) {
      return null;
    }

    ClientRequest knownRequest = pendingRequests.get(requestKey);
    if (knownRequest != null) {
      if (!knownRequest.equals(request)) {
        return null;
      }
      if (key != null) {
        clientContexts.putIfAbsent(requestKey, key);
      }
      return knownRequest;
    }

    if (key != null) {
      clientContexts.putIfAbsent(requestKey, key);
    }
    pendingRequests.putIfAbsent(requestKey, request);
    return request;
  }

  private int enqueuePendingRequests() {
    int enqueuedRequests = 0;
    for (ClientRequest request : pendingRequests.values()) {
      if (enqueueIfNotQueued(request)) {
        enqueuedRequests++;
      }
    }
    return enqueuedRequests;
  }

  private QueuedClientRequest pollPendingCommand(long deadlineNanos) {
    while (!TimeUtil.hasTimedOutMonotonic(deadlineNanos)) {
      try {
        QueuedClientRequest nextRequest = pendingCommands.poll(TimeUtil.monotonicRemainingMsUntil(deadlineNanos), TimeUnit.MILLISECONDS);
        if (nextRequest != null) {
          return nextRequest;
        }
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new ConsensusTimeoutException("Interrupted while waiting for client command");
      }
    }

    return null;
  }

  private boolean isValidClientRequestSignature(ClientRequest request) {
    ClientRequestKey requestKey = requestKeyOrNull(request);
    if (request == null || requestKey == null) {
      return false;
    }
    PublicKey clientPublicKey = clientPublicKeys.get(requestKey.getClientSenderId());
    if (clientPublicKey == null) {
      return false;
    }

    try {
      if (request.hasTransaction()) {
        TransactionRequest transaction = request.getTransaction();
        byte[] payload = ClientRequestSignaturePayloadUtil
            .signedTransactionRequestPayload(requestKey.getClientSenderId(), requestKey.getRequestId(), transaction.getType(), transaction.getTo(), transaction
                .getAmount(), transaction
                    .getNonce(), transaction.getGasLimit(), transaction.getGasPrice(), transaction.hasInput() ? transaction.getInput().toByteArray() : new byte[0]);
        return CryptoUtil.verifyEcdsa(payload, transaction.getSignature().toByteArray(), clientPublicKey);
      }

      if (request.hasQuery()) {
        QueryRequest query = request.getQuery();
        byte[] payload = ClientRequestSignaturePayloadUtil.signedQueryRequestPayload(requestKey.getClientSenderId(), requestKey.getRequestId(), query.getType(), query.getOwner());
        return CryptoUtil.verifyEcdsa(payload, query.getSignature().toByteArray(), clientPublicKey);
      }
    } catch (Exception exception) {
      return false;
    }
    return false;
  }

  private boolean enqueueIfNotQueued(ClientRequest request) {
    ClientRequestKey requestKey = requestKeyOrNull(request);
    if (requestKey == null) {
      return false;
    }
    if (completedRequestIds.contains(requestKey)) {
      return false;
    }

    if (queuedRequestIds.add(requestKey)) {
      pendingCommands.offer(new QueuedClientRequest(request, enqueueSequence.getAndIncrement()));
      return true;
    }
    return false;
  }

  private boolean canAcceptAllRequests(List<ClientRequest> requests) {
    for (ClientRequest request : requests) {
      ClientRequestKey requestKey = requestKeyOrNull(request);
      if (request == null || requestKey == null) {
        return false;
      }
      if (completedRequestIds.contains(requestKey)) {
        return false;
      }

      ClientRequest knownRequest = pendingRequests.get(requestKey);
      if (knownRequest != null) {
        if (!knownRequest.equals(request)) {
          return false;
        }
        continue;
      }

      if (!isValidClientRequest(request)) {
        return false;
      }
    }
    return true;
  }

  private boolean fitsBlockGasPolicy(ClientRequest request) {
    if (request == null || !request.hasTransaction()) {
      return true;
    }
    return request.getTransaction().getGasLimit() <= BlockStore.MAX_BLOCK_GAS_LIMIT;
  }

  private ClientRequest activeRequestOrNull(QueuedClientRequest queuedRequest) {
    if (queuedRequest == null) {
      return null;
    }

    ClientRequest request = queuedRequest.request();
    ClientRequestKey requestKey = requestKeyOrNull(request);
    if (requestKey == null) {
      return null;
    }

    if (completedRequestIds.contains(requestKey)) {
      queuedRequestIds.remove(requestKey);
      pendingRequests.remove(requestKey);
      return null;
    }

    ClientRequest knownRequest = pendingRequests.get(requestKey);
    if (knownRequest == null || !knownRequest.equals(request)) {
      queuedRequestIds.remove(requestKey);
      return null;
    }

    return knownRequest;
  }

  private void selectQueuedRequest(ClientRequest request) {
    ClientRequestKey requestKey = requestKeyOrNull(request);
    if (requestKey != null) {
      queuedRequestIds.remove(requestKey);
    }
  }

  private static long gasLimitOf(ClientRequest request) {
    if (request != null && request.hasTransaction()) {
      return request.getTransaction().getGasLimit();
    }
    return 0L;
  }

  private static ClientRequestKey requestKeyOrNull(NodeCommand command) {
    List<ClientRequest> requests = clientRequests(command);
    if (requests.size() == 1) {
      return requestKeyOrNull(requests.getFirst());
    }
    return null;
  }

  private static ClientRequestKey requestKeyOrNull(ClientRequest request) {
    if (request == null) {
      return null;
    }
    if (request.hasTransaction()) {
      return request.getTransaction().getRequestKey();
    }
    if (request.hasQuery()) {
      return request.getQuery().getRequestKey();
    }
    return null;
  }

  private static List<ClientRequest> clientRequests(NodeCommand command) {
    if (command == null) {
      return List.of();
    }
    if (command.hasTransactionBatch()) {
      return List.copyOf(command.getTransactionBatch().getClientRequestsList());
    }
    return List.of();
  }

  public record ExecutionContext(ClientRequest request, ConnectionKey replyTarget) {
  }

  private record QueuedClientRequest(ClientRequest request, long enqueueSequence) {
    private static final Comparator<QueuedClientRequest> ORDERING = Comparator.comparingLong(QueuedClientRequest::gasPrice).reversed()
        .thenComparingLong(QueuedClientRequest::enqueueSequence);

    private long gasPrice() {
      if (request.hasTransaction()) {
        return request.getTransaction().getGasPrice();
      }
      return Long.MIN_VALUE;
    }
  }
}
