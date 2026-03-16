package pt.ulisboa.depchain.server.consensus;

import java.security.PublicKey;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

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

  void onClientRequest(ClientRequest request, ConnectionKey key, boolean isLeader) {
    ClientRequest acceptedRequest = registerKnownRequest(request, key);
    if (acceptedRequest != null && isLeader) {
      enqueueIfNotQueued(acceptedRequest);
    }
  }

  boolean observeProposedCommand(NodeCommand command) {
    if (command == null || !command.hasAppend()) {
      return true;
    }

    ClientRequest request = command.getAppend().getClientRequest();
    return registerKnownRequest(request, null) != null;
  }

  int pendingCount() {
    return pendingRequests.size();
  }

  int enqueuePendingKnownRequestsIfLeader(boolean isLeader) {
    if (!isLeader || pendingRequests.isEmpty()) {
      return 0;
    }
    return enqueuePendingRequests();
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
    ClientRequestKey requestKey = requestKeyOrNull(command);
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

  private ClientRequest registerKnownRequest(ClientRequest request, ConnectionKey key) {
    if (!hasValidClientRequest(request)) {
      return null;
    }

    ClientRequestKey requestKey = request.getAppend().getRequestKey();
    if (completedRequestIds.contains(requestKey)) {
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

  private static ClientRequestKey requestKeyOrNull(NodeCommand command) {
    if (command == null || !command.hasAppend() || !command.getAppend().hasClientRequest() || !command.getAppend().getClientRequest().hasAppend()) {
      return null;
    }
    return command.getAppend().getClientRequest().getAppend().getRequestKey();
  }

  record ExecutionResult(String commandValue, ConnectionKey replyTarget) {
  }
}
