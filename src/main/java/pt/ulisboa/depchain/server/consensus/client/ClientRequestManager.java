package pt.ulisboa.depchain.server.consensus.client;

import java.security.PublicKey;
import java.util.Comparator;
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
import pt.ulisboa.depchain.server.consensus.hotstuff.HotStuffSupport;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.utils.ClientRequestSignaturePayloadUtil;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;
import pt.ulisboa.depchain.shared.utils.ProtoValidationUtil;
import pt.ulisboa.depchain.shared.utils.TimeUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class ClientRequestManager {
  private final Map<Long, PublicKey> clientPublicKeys;
  private final Logger logger;
  private final BlockingQueue<QueuedClientRequest> pendingCommands = new PriorityBlockingQueue<>(11, QueuedClientRequest.ORDERING);
  private final Map<ClientRequestKey, ConnectionKey> clientContexts = new ConcurrentHashMap<>();
  private final Map<ClientRequestKey, ClientRequest> pendingRequests = new ConcurrentHashMap<>();
  private final Set<ClientRequestKey> queuedRequestIds = ConcurrentHashMap.newKeySet();
  private final Set<ClientRequestKey> completedRequestIds = ConcurrentHashMap.newKeySet();
  private final AtomicLong enqueueSequence = new AtomicLong();
  private AuthenticatedLink clientTransport;

  public ClientRequestManager(long expectedClientSenderId, PublicKey clientPublicKey, Logger logger) {
    this(Map.of(expectedClientSenderId, ValidationUtils.requireNonNull(clientPublicKey, "clientPublicKey")), logger);
  }

  public ClientRequestManager(Map<Long, PublicKey> clientPublicKeys, Logger logger) {
    ValidationUtils.requireNonNull(clientPublicKeys, "clientPublicKeys");
    if (clientPublicKeys.isEmpty()) {
      throw new IllegalArgumentException("clientPublicKeys must not be empty");
    }
    this.clientPublicKeys = Map.copyOf(clientPublicKeys);
    this.logger = ValidationUtils.requireNonNull(logger, "logger");
  }

  public void init(AuthenticatedLink clientTransport) {
    this.clientTransport = clientTransport;
  }

  public void onClientRequest(ClientRequest request, ConnectionKey key, boolean isLeader) {
    ClientRequest acceptedRequest = registerKnownRequest(request, key);
    if (acceptedRequest != null && isLeader) {
      enqueueIfNotQueued(acceptedRequest);
    }
  }

  public boolean observeProposedCommand(NodeCommand command) {
    ClientRequest request = clientRequestOrNull(command);
    if (request == null) {
      return true;
    }
    return registerKnownRequest(request, null) != null;
  }

  public int pendingCount() {
    return pendingRequests.size();
  }

  public int enqueuePendingKnownRequestsIfLeader(boolean isLeader) {
    if (!isLeader || pendingRequests.isEmpty()) {
      return 0;
    }
    return enqueuePendingRequests();
  }

  public ClientRequest awaitNextPending(long deadlineNanos) {
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

  public ExecutionResult markExecuted(Node node) {
    NodeCommand command = node.getCommand();
    String commandSummary = HotStuffSupport.commandSummary(command);
    ClientRequestKey requestKey = requestKeyOrNull(command);
    if (requestKey == null) {
      return new ExecutionResult(commandSummary, null);
    }

    completedRequestIds.add(requestKey);
    pendingRequests.remove(requestKey);
    queuedRequestIds.remove(requestKey);
    return new ExecutionResult(commandSummary, clientContexts.remove(requestKey));
  }

  public void replyToClient(ConnectionKey key, ClientResponse response) {
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

  public boolean hasValidClientRequest(ClientRequest request) {
    return hasValidClientRequestSignature(request);
  }

  private ClientRequest registerKnownRequest(ClientRequest request, ConnectionKey key) {
    ClientRequestKey requestKey = requestKeyOrNull(request);
    if (requestKey == null) {
      return null;
    }

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

    if (!hasValidClientRequestSignature(request)) {
      return null;
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

  private boolean hasValidClientRequestSignature(ClientRequest request) {
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

  private static ClientRequestKey requestKeyOrNull(NodeCommand command) {
    return requestKeyOrNull(clientRequestOrNull(command));
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

  private static ClientRequest clientRequestOrNull(NodeCommand command) {
    if (command == null) {
      return null;
    }
    if (command.hasTransaction() && command.getTransaction().hasClientRequest()) {
      return command.getTransaction().getClientRequest();
    }
    return null;
  }

  public record ExecutionResult(String commandSummary, ConnectionKey replyTarget) {
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
