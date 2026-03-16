package pt.ulisboa.depchain.server.consensus;

import java.security.PublicKey;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;

import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.ClientResponse;
import pt.ulisboa.depchain.proto.Node;
import pt.ulisboa.depchain.proto.NodeCommand;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.utils.ClientRequestPayloadUtil;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;
import pt.ulisboa.depchain.shared.utils.ProtoValidationUtil;
import pt.ulisboa.depchain.shared.utils.TimeUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class ClientCommunicationManager {
  private final long expectedClientSenderId;
  private final PublicKey clientPublicKey;
  private final Logger logger;
  private final BlockingQueue<ClientRequest> pendingCommands = new LinkedBlockingQueue<>();
  private final Map<ClientRequestKey, ConnectionKey> clientContexts = new ConcurrentHashMap<>();
  private final Map<ClientRequestKey, ClientRequest> pendingRequests = new ConcurrentHashMap<>();
  private final Set<ClientRequestKey> queuedRequestIds = ConcurrentHashMap.newKeySet();
  private final Set<ClientRequestKey> completedRequestIds = ConcurrentHashMap.newKeySet();
  private AuthenticatedLink clientTransport;

  ClientCommunicationManager(long expectedClientSenderId, PublicKey clientPublicKey, Logger logger) {
    this.expectedClientSenderId = expectedClientSenderId;
    this.clientPublicKey = ValidationUtils.requireNonNull(clientPublicKey, "clientPublicKey");
    this.logger = ValidationUtils.requireNonNull(logger, "logger");
  }

  void init(AuthenticatedLink clientTransport) {
    this.clientTransport = clientTransport;
  }

  void receiveClientRequest(ClientRequest request, ConnectionKey key, boolean isLeader, Consumer<ClientRequest> forwardAction) {
    ClientRequest acceptedRequest = registerClientRequest(request, key);
    if (acceptedRequest != null) {
      submitRequest(acceptedRequest, isLeader, forwardAction);
    }
  }

  void receiveForwardedRequest(ClientRequest request, boolean isLeader) {
    ClientRequest acceptedRequest = registerForwardedRequest(request);
    if (acceptedRequest != null && isLeader) {
      enqueueIfNotQueued(acceptedRequest);
    }
  }

  int pendingCount() {
    return pendingRequests.size();
  }

  int resubmitPendingRequests(boolean isLeader, Consumer<ClientRequest> forwardAction) {
    if (pendingRequests.isEmpty()) {
      return 0;
    }
    if (isLeader) {
      return enqueuePendingRequests();
    }

    int forwardedRequests = 0;
    for (ClientRequest request : pendingRequests.values()) {
      forwardAction.accept(request);
      forwardedRequests++;
    }
    return forwardedRequests;
  }

  ClientRequest awaitNextPending(long deadlineNanos) {
    while (true) {
      ClientRequest nextRequest = pollPendingCommand(deadlineNanos);
      if (nextRequest == null) {
        return null;
      }

      ClientRequestKey requestKey = nextRequest.getAppend().getRequestKey();
      queuedRequestIds.remove(requestKey);
      if (!completedRequestIds.contains(requestKey) && pendingRequests.containsKey(requestKey)) {
        return nextRequest;
      }
    }
  }

  private void submitRequest(ClientRequest request, boolean isLeader, Consumer<ClientRequest> forwardAction) {
    if (isLeader) {
      enqueueIfNotQueued(request);
      return;
    }

    forwardAction.accept(request);
  }

  private boolean enqueueIfNotQueued(ClientRequest request) {
    ClientRequestKey requestKey = request.getAppend().getRequestKey();
    if (completedRequestIds.contains(requestKey)) {
      return false;
    }

    if (queuedRequestIds.add(requestKey)) {
      pendingCommands.offer(request);
      return true;
    }
    return false;
  }

  ExecutionResult markExecuted(Node node) {
    NodeCommand command = node.getCommand();
    String commandValue = ConsensusUtil.commandValue(command);
    ClientRequestKey requestKey = command.hasAppend() ? command.getAppend().getClientRequestKey() : null;
    if (requestKey == null) {
      return new ExecutionResult(commandValue, null);
    }

    completedRequestIds.add(requestKey);
    pendingRequests.remove(requestKey);
    queuedRequestIds.remove(requestKey);
    return new ExecutionResult(commandValue, clientContexts.remove(requestKey));
  }

  void replyToClient(ConnectionKey key, ClientResponse response) {
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

  private ClientRequest registerClientRequest(ClientRequest request, ConnectionKey key) {
    if (!hasValidClientRequest(request)) {
      return null;
    }

    ClientRequestKey requestKey = request.getAppend().getRequestKey();
    if (completedRequestIds.contains(requestKey)) {
      return null;
    }

    clientContexts.putIfAbsent(requestKey, key);
    pendingRequests.putIfAbsent(requestKey, request);
    return request;
  }

  private ClientRequest registerForwardedRequest(ClientRequest request) {
    if (!hasValidClientRequest(request)) {
      return null;
    }

    ClientRequestKey requestKey = request.getAppend().getRequestKey();
    if (completedRequestIds.contains(requestKey)) {
      return null;
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

  private ClientRequest pollPendingCommand(long deadlineNanos) {
    while (!TimeUtil.hasTimedOutMonotonic(deadlineNanos)) {
      try {
        ClientRequest nextRequest = pendingCommands.poll(TimeUtil.monotonicRemainingMsUntil(deadlineNanos), TimeUnit.MILLISECONDS);
        if (nextRequest != null) {
          return nextRequest;
        }
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new ViewChangeTimeoutException("Interrupted while waiting for client command");
      }
    }

    return null;
  }

  private boolean hasValidClientRequest(ClientRequest request) {
    if (request == null || !request.hasAppend()) {
      return false;
    }

    ClientRequestKey requestKey = request.getAppend().getRequestKey();
    if (requestKey.getClientSenderId() != expectedClientSenderId) {
      return false;
    }

    try {
      byte[] payload = ClientRequestPayloadUtil.signedAppendRequestPayload(requestKey.getClientSenderId(), requestKey.getRequestId(), request.getAppend().getValue());
      return CryptoUtil.verifyEcdsa(payload, request.getAppend().getSignature().toByteArray(), clientPublicKey);
    } catch (Exception exception) {
      return false;
    }
  }

  record ExecutionResult(String commandValue, ConnectionKey replyTarget) {
  }
}
