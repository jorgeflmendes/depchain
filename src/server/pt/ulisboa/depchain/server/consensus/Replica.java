package pt.ulisboa.depchain.server.consensus;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayDeque;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.weavechain.curve25519.Scalar;

import pt.ulisboa.depchain.proto.AppendNodeCommand;
import pt.ulisboa.depchain.proto.AppendResponse;
import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.ClientResponse;
import pt.ulisboa.depchain.proto.ConsensusMessageType;
import pt.ulisboa.depchain.proto.ForwardedRequestMessage;
import pt.ulisboa.depchain.proto.Message;
import pt.ulisboa.depchain.proto.Node;
import pt.ulisboa.depchain.proto.NodeCommand;
import pt.ulisboa.depchain.proto.PhaseCertificateMessage;
import pt.ulisboa.depchain.proto.ProposalMessage;
import pt.ulisboa.depchain.proto.QuorumCertificate;
import pt.ulisboa.depchain.server.consensus.threshold.ThresholdSignatureProtocol;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.utils.ClientRequestPayloadUtil;
import pt.ulisboa.depchain.shared.utils.ConsensusPayloadUtil;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;
import pt.ulisboa.depchain.shared.utils.ProtoValidationUtil;
import pt.ulisboa.depchain.shared.utils.TimeUtil;

public class Replica {
  private ConfigParser config;
  private PublicKey clientPublicKey;

  private AuthenticatedLink nodeTransport;
  private AuthenticatedLink clientTransport;

  private final Map<ClientRequestKey, ConnectionKey> clientContexts;
  private final Map<ClientRequestKey, ClientRequest> pendingForwardedRequests;
  private final Set<ClientRequestKey> acceptedRequestIds;
  private final Set<ClientRequestKey> completedRequestIds;

  private int id;
  private int n;
  private int f;
  private int quorum;
  private long viewChangeTimeoutMs;

  private int viewNumber;
  private QuorumCertificate highQC;
  private QuorumCertificate lockedQC;
  private QuorumCertificate prepareQC;
  private Node currentProposal;
  private Map<String, Node> blockTree;
  private final Map<Integer, InetSocketAddress> consensusEndpointsBySenderId;

  private BlockingDeque<Message> messageQueue;
  private final BlockingQueue<ClientRequest> pendingCommands;

  private ThresholdSignatureProtocol thresholdProtocol;
  private final Logger logger;

  public Replica(int id, ConfigParser config, Scalar localThresholdShare, byte[] publicThresholdKey, PublicKey clientPublicKey) {
    this.config = config;
    this.clientContexts = new ConcurrentHashMap<>();
    this.clientPublicKey = clientPublicKey;
    this.pendingForwardedRequests = new ConcurrentHashMap<>();
    this.acceptedRequestIds = ConcurrentHashMap.newKeySet();
    this.completedRequestIds = ConcurrentHashMap.newKeySet();

    this.id = id;
    this.logger = LoggerFactory.getLogger(Replica.class);
    this.n = config.system().n();
    this.f = config.system().f();
    this.quorum = n - f;
    this.viewChangeTimeoutMs = config.timeouts().viewChangeMs();

    this.thresholdProtocol = new ThresholdSignatureProtocol(id, config, localThresholdShare, publicThresholdKey);
    this.highQC = thresholdProtocol.genesisQC();
    this.lockedQC = thresholdProtocol.genesisQC();
    this.prepareQC = thresholdProtocol.genesisQC();
    this.currentProposal = ConsensusUtil.GENESIS_NODE;
    this.viewNumber = 0;
    this.blockTree = new HashMap<>();
    this.consensusEndpointsBySenderId = buildConsensusEndpointsBySenderId(config);

    this.messageQueue = new LinkedBlockingDeque<>();
    this.pendingCommands = new LinkedBlockingQueue<>();

    blockTree.put(ConsensusUtil.GENESIS_NODE.getNodeHash(), ConsensusUtil.GENESIS_NODE);
  }

  public void initNetwork(AuthenticatedLink nodeTransport, AuthenticatedLink clientTransport) {
    this.nodeTransport = nodeTransport;
    this.clientTransport = clientTransport;
    this.thresholdProtocol.initTransport(nodeTransport);
  }

  public void run() {
    logger.info("Starting at view {}", viewNumber);
    sendToLeader(newPhaseCertificateMessage(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_NEW_VIEW, prepareQC));

    while (true) {
      try {
        onPrepare();
        onPreCommit();
        onCommit();
        onDecide();
        onNewView();
      } catch (ViewChangeTimeoutException e) {
        onNewView();
      }
    }
  }

  public void receiveMessage(Message msg) {
    if (msg.getMessageType() == ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_FORWARDED_REQUEST) {
      receiveForwardedRequest(msg.hasForwardedRequest() ? msg.getForwardedRequest().getClientRequest() : null);
      return;
    }

    observeMessage(msg);
    messageQueue.add(msg);
  }

  public void receiveClientCommand(ClientRequest request, ConnectionKey key) {
    if (!hasValidClientRequest(request)) {
      return;
    }

    ClientRequestKey requestKey = request.getAppend().getRequestKey();
    if (completedRequestIds.contains(requestKey)) {
      return;
    }

    clientContexts.putIfAbsent(requestKey, key);
    pendingForwardedRequests.putIfAbsent(requestKey, request);
    submitClientRequest(request);
  }

  private void onPrepare() {
    if (isLeader()) {
      long deadlineNanos = TimeUtil.monotonicDeadlineAfterNow(viewChangeTimeoutMs);
      List<Message> msgs = waitForQuorumMessagesUntil(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_NEW_VIEW, viewNumber, quorum - 1, deadlineNanos);
      QuorumCertificate selectedHighQC = highQC;
      for (Message m : msgs) {
        QuorumCertificate justifyQc = m.hasPhaseCertificate() ? m.getPhaseCertificate().getJustifyQc() : null;
        if (justifyQc != null && justifyQc.getViewNumber() > selectedHighQC.getViewNumber()) {
          selectedHighQC = justifyQc;
        }
      }
      updateHighQC(selectedHighQC);

      ClientRequest request = awaitNextCommandUntil(deadlineNanos);
      if (request == null) {
        throw new ViewChangeTimeoutException("Timed out waiting for client command");
      }

      NodeCommand command = NodeCommand.newBuilder()
          .setAppend(AppendNodeCommand.newBuilder().setClientRequestKey(request.getAppend().getRequestKey()).setValue(request.getAppend().getValue()))
          .build();
      currentProposal = createLeaf(selectedHighQC.getCertifiedNode().getNodeHash(), command);
      observeNode(currentProposal);

      Message proposal = Message.newBuilder().setViewNumber(viewNumber).setReplicaSenderId(id).setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE)
          .setProposal(ProposalMessage.newBuilder().setProposedNode(currentProposal).setJustifyQc(selectedHighQC)).build();
      broadcast(proposal);
    } else {
      Message prepareMsg = waitForMessageFromSender(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, viewNumber, getLeader(viewNumber));
      if (prepareMsg == null) {
        return;
      }

      QuorumCertificate justify = prepareMsg.hasProposal() ? prepareMsg.getProposal().getJustifyQc() : null;
      Node proposedNode = prepareMsg.hasProposal() ? prepareMsg.getProposal().getProposedNode() : null;

      boolean hasValidJustify = justify != null && verifyQC(justify);
      boolean hasValidProposedNode = proposedNode != null && hasValidNode(proposedNode);
      boolean extendsJustifiedNode = justify != null && proposedNode != null && ConsensusUtil.extendsNode(proposedNode, justify.getCertifiedNode());
      boolean isSafeProposal = proposedNode != null && safeNode(proposedNode, justify);
      if (hasValidJustify && hasValidProposedNode && extendsJustifiedNode && isSafeProposal) {
        observeQuorumCertificate(justify);
        observeNode(proposedNode);
        currentProposal = proposedNode;
        sendToLeader(voteMessage(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, currentProposal));
      }
    }
  }

  private void onPreCommit() {
    if (isLeader()) {
      prepareQC = buildQC(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, currentProposal);
      updateHighQC(prepareQC);
      broadcast(newPhaseCertificateMessage(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT, prepareQC));
    } else {
      Message msg = waitForQCMessage(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, viewNumber, getLeader(viewNumber));
      prepareQC = msg.getPhaseCertificate().getJustifyQc();
      updateHighQC(prepareQC);
      sendToLeader(voteMessage(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT, prepareQC.getCertifiedNode()));
    }
  }

  private void onCommit() {
    if (isLeader()) {
      QuorumCertificate preCommitQC = buildQC(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT, currentProposal);
      lockedQC = preCommitQC;
      updateHighQC(preCommitQC);
      broadcast(newPhaseCertificateMessage(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_COMMIT, preCommitQC));
    } else {
      Message msg = waitForQCMessage(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT, viewNumber, getLeader(viewNumber));
      lockedQC = msg.getPhaseCertificate().getJustifyQc();
      updateHighQC(lockedQC);
      sendToLeader(voteMessage(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_COMMIT, lockedQC.getCertifiedNode()));
    }
  }

  private void onDecide() {
    if (isLeader()) {
      QuorumCertificate commitQC = buildQC(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_COMMIT, currentProposal);
      updateHighQC(commitQC);
      broadcast(newPhaseCertificateMessage(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_DECIDE, commitQC));
      executeCommand(currentProposal);
    } else {
      Message msg = waitForQCMessage(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_COMMIT, viewNumber, getLeader(viewNumber));
      QuorumCertificate decideQC = msg.getPhaseCertificate().getJustifyQc();
      updateHighQC(decideQC);
      executeCommand(decideQC.getCertifiedNode());
    }
  }

  private void onNewView() {
    viewNumber++;
    resubmitClientCommands();
    sendToLeader(newPhaseCertificateMessage(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_NEW_VIEW, prepareQC));
  }

  private void executeCommand(Node node) {
    NodeCommand command = node.getCommand();
    String clientCommand = ConsensusUtil.commandValue(command);
    ClientRequestKey requestKey = command.hasAppend() ? command.getAppend().getClientRequestKey() : null;

    if (!ConsensusUtil.isNoOp(command)) {
      logger.debug("Executing command: {}", clientCommand);
    }
    observeNode(node);
    if (requestKey == null) {
      return;
    }

    completedRequestIds.add(requestKey);
    pendingForwardedRequests.remove(requestKey);
    ConnectionKey key = clientContexts.remove(requestKey);
    if (key != null && clientTransport != null) {
      try {
        ClientResponse clientResponse = ClientResponse.newBuilder().setAppend(AppendResponse.newBuilder().setSuccess(true).setMessage("Success: " + clientCommand)).build();
        byte[] response = ProtoValidationUtil.requireValid(clientResponse, "ClientResponse").toByteArray();
        clientTransport.send(key.connectionId(), response, key.endpoint());
      } catch (Exception e) {
        logger.error("Error replying to client connection", e);
      }
    }
  }

  private void broadcast(Message msg) {
    byte[] payload = ProtoValidationUtil.requireValid(msg, "ReplicaMessage").toByteArray();

    for (ConfigParser.ReplicaSection peer : config.replicas()) {
      if (peer.senderId() == id) {
        continue;
      }

      try {
        nodeTransport.send(0L, payload, requireConsensusEndpoint(peer.senderId()));
      } catch (Exception e) {
        logger.error("Error in broadcast to {}", peer.id(), e);
      }
    }
  }

  private void sendToLeader(Message msg) {
    try {
      int leaderId = getLeader(viewNumber);
      if (leaderId == id) {
        receiveMessage(msg);
        return;
      }

      byte[] payload = ProtoValidationUtil.requireValid(msg, "ReplicaMessage").toByteArray();
      nodeTransport.send(0L, payload, requireConsensusEndpoint(leaderId));
    } catch (Exception e) {
      logger.error("Error sending message to leader", e);
    }
  }

  private Message voteMessage(ConsensusMessageType type, Node node) {
    return thresholdProtocol.createVoteMessage(viewNumber, type, node, getLeader(viewNumber), messageQueue);
  }

  private QuorumCertificate buildQC(ConsensusMessageType type, Node node) {
    return thresholdProtocol.buildQC(viewNumber, type, node, messageQueue);
  }

  private boolean verifyQC(QuorumCertificate qc) {
    return thresholdProtocol.verifyQC(qc);
  }

  private List<Message> waitForQuorumMessagesUntil(ConsensusMessageType type, int view, int requiredCount, long deadlineNanos) {
    LinkedHashMap<Integer, Message> msgsBySender = new LinkedHashMap<>();
    ArrayDeque<Message> deferredMessages = new ArrayDeque<>();
    try {
      while (msgsBySender.size() < requiredCount) {
        Message msg = pollMessageUntil(deadlineNanos, "waiting for " + type + " messages");

        if (matchingMSG(msg, type, view) && !thresholdProtocol.isAuxiliaryMessage(msg)) {
          msgsBySender.putIfAbsent(msg.getReplicaSenderId(), msg);
        } else {
          deferredMessages.addLast(msg);
        }
      }

      return new ArrayList<>(msgsBySender.values());
    } finally {
      restoreDeferredMessages(deferredMessages);
    }
  }

  private Message waitForMessageFromSender(ConsensusMessageType type, int view, int sender) {
    long deadlineNanos = TimeUtil.monotonicDeadlineAfterNow(viewChangeTimeoutMs);
    ArrayDeque<Message> deferredMessages = new ArrayDeque<>();
    try {
      while (true) {
        Message msg = pollMessageUntil(deadlineNanos, "waiting for " + type + " from " + sender);
        if (msg.getReplicaSenderId() == sender && matchingMSG(msg, type, view) && !thresholdProtocol.isAuxiliaryMessage(msg)) {
          return msg;
        }
        deferredMessages.addLast(msg);
      }
    } finally {
      restoreDeferredMessages(deferredMessages);
    }
  }

  private Message waitForQCMessage(ConsensusMessageType type, int view, int sender) {
    long deadlineNanos = TimeUtil.monotonicDeadlineAfterNow(viewChangeTimeoutMs);
    ArrayDeque<Message> deferredMessages = new ArrayDeque<>();
    try {
      while (true) {
        Message msg = pollMessageUntil(deadlineNanos, "waiting for QC " + type);
        QuorumCertificate justifyQc = msg.hasPhaseCertificate() ? msg.getPhaseCertificate().getJustifyQc() : null;
        if (matchingQC(justifyQc, type, view) && msg.getReplicaSenderId() == sender && !thresholdProtocol.isAuxiliaryMessage(msg) && verifyQC(justifyQc)) {
          return msg;
        }
        deferredMessages.addLast(msg);
      }
    } finally {
      restoreDeferredMessages(deferredMessages);
    }
  }

  private Message pollMessageUntil(long deadlineNanos, String description) {
    if (TimeUtil.hasTimedOutMonotonic(deadlineNanos)) {
      throw new ViewChangeTimeoutException("Timed out " + description);
    }

    try {
      long remainingMs = TimeUtil.monotonicRemainingMsUntil(deadlineNanos);
      Message msg = messageQueue.poll(remainingMs, TimeUnit.MILLISECONDS);
      if (msg == null) {
        throw new ViewChangeTimeoutException("Timed out " + description);
      }
      return msg;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ViewChangeTimeoutException("Interrupted while " + description);
    }
  }

  private Node createLeaf(String parentHash, NodeCommand command) {
    byte[] hashPayload = ConsensusPayloadUtil.nodeHashPayload(parentHash, viewNumber, command);
    String thisHash = CryptoUtil.sha256Hex(hashPayload);
    return Node.newBuilder().setParentNodeHash(parentHash).setNodeHash(thisHash).setViewNumber(viewNumber).setCommand(command).build();
  }

  private boolean safeNode(Node node, QuorumCertificate qc) {
    if (node == null || qc == null) {
      return false;
    }
    return extendsKnownBranch(node, lockedQC.getCertifiedNode()) || qc.getViewNumber() > lockedQC.getViewNumber();
  }

  private int getLeader(int view) {
    return view % n;
  }

  private void submitClientRequest(ClientRequest request) {
    if (isLeader()) {
      enqueueCommandIfNew(request);
    } else {
      forwardClientRequestToLeader(request);
    }
  }

  private void receiveForwardedRequest(ClientRequest request) {
    if (request == null) {
      return;
    }

    if (!isLeader()) {
      forwardClientRequestToLeader(request);
      return;
    }

    if (hasValidClientRequest(request)) {
      enqueueCommandIfNew(request);
    }
  }

  private void resubmitClientCommands() {
    if (pendingForwardedRequests.isEmpty()) {
      return;
    }

    if (isLeader()) {
      for (ClientRequest request : pendingForwardedRequests.values()) {
        enqueueCommandIfNew(request);
      }
      return;
    }

    for (ClientRequest request : pendingForwardedRequests.values()) {
      forwardClientRequestToLeader(request);
    }
  }

  private void enqueueCommandIfNew(ClientRequest request) {
    ClientRequestKey requestKey = request.getAppend().getRequestKey();
    if (completedRequestIds.contains(requestKey)) {
      return;
    }

    if (acceptedRequestIds.add(requestKey)) {
      pendingCommands.offer(request);
    }
  }

  private ClientRequest awaitNextCommandUntil(long deadlineNanos) {
    while (true) {
      ClientRequest nextRequest = pollPendingCommand(deadlineNanos);
      if (nextRequest == null) {
        return null;
      }
      if (!completedRequestIds.contains(nextRequest.getAppend().getRequestKey())) {
        return nextRequest;
      }
    }
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

  private void forwardClientRequestToLeader(ClientRequest request) {
    Message forwardedRequest = Message.newBuilder().setViewNumber(viewNumber).setReplicaSenderId(id)
        .setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_FORWARDED_REQUEST)
        .setForwardedRequest(ForwardedRequestMessage.newBuilder().setClientRequest(request)).build();
    sendToLeader(forwardedRequest);
  }

  private boolean hasValidClientRequest(ClientRequest request) {
    if (request == null || !request.hasAppend()) {
      return false;
    }

    ClientRequestKey requestKey = request.getAppend().getRequestKey();
    if (requestKey.getClientSenderId() != config.client().senderId()) {
      return false;
    }

    try {
      byte[] payload = ClientRequestPayloadUtil.signedAppendRequestPayload(requestKey.getClientSenderId(), requestKey.getRequestId(), request.getAppend().getValue());
      return CryptoUtil.verifyEcdsa(payload, request.getAppend().getSignature().toByteArray(), clientPublicKey);
    } catch (Exception exception) {
      return false;
    }
  }

  private boolean isLeader() {
    return getLeader(viewNumber) == id;
  }

  private Message newPhaseCertificateMessage(ConsensusMessageType type, QuorumCertificate justifyQc) {
    return Message.newBuilder().setViewNumber(viewNumber).setReplicaSenderId(id).setMessageType(type)
        .setPhaseCertificate(PhaseCertificateMessage.newBuilder().setJustifyQc(justifyQc)).build();
  }

  private boolean matchingMSG(Message m, ConsensusMessageType t, int v) {
    return m.getMessageType() == t && m.getViewNumber() == v;
  }

  private boolean matchingQC(QuorumCertificate qc, ConsensusMessageType t, int v) {
    return qc != null && qc.getMessageType() == t && qc.getViewNumber() == v;
  }

  private void observeMessage(Message msg) {
    if (msg == null) {
      return;
    }

    switch (msg.getBodyCase()) {
      case PROPOSAL -> {
        observeQuorumCertificateIfValid(msg.getProposal().getJustifyQc());
        observeNode(msg.getProposal().getProposedNode());
      }
      case PHASE_CERTIFICATE -> observeQuorumCertificateIfValid(msg.getPhaseCertificate().getJustifyQc());
      case VOTE -> observeNode(msg.getVote().getVotedNode());
      case COMMITMENT -> observeNode(msg.getCommitment().getVotedNode());
      case THRESHOLD_CONTEXT -> observeNode(msg.getThresholdContext().getVotedNode());
      default -> {
      }
    }
  }

  private void observeQuorumCertificateIfValid(QuorumCertificate qc) {
    if (qc != null && verifyQC(qc)) {
      observeQuorumCertificate(qc);
    }
  }

  private void observeQuorumCertificate(QuorumCertificate qc) {
    if (qc == null) {
      return;
    }

    observeNode(qc.getCertifiedNode());
    updateHighQC(qc);
  }

  private void updateHighQC(QuorumCertificate candidateQc) {
    if (candidateQc == null) {
      return;
    }

    if (highQC == null || candidateQc.getViewNumber() > highQC.getViewNumber()) {
      highQC = candidateQc;
    }
  }

  private void observeNode(Node node) {
    if (node == null || !hasValidNode(node)) {
      return;
    }

    blockTree.putIfAbsent(node.getNodeHash(), node);
  }

  private boolean hasValidNode(Node node) {
    if (node == null) {
      return false;
    }
    if (ConsensusUtil.isSameNode(node, ConsensusUtil.GENESIS_NODE)) {
      return true;
    }

    byte[] expectedHashPayload = ConsensusPayloadUtil.nodeHashPayload(node.getParentNodeHash(), node.getViewNumber(), node.getCommand());
    return node.getNodeHash().equals(CryptoUtil.sha256Hex(expectedHashPayload));
  }

  private boolean extendsKnownBranch(Node node, Node ancestor) {
    if (node == null || ancestor == null) {
      return false;
    }
    if (ConsensusUtil.isSameNode(node, ancestor)) {
      return true;
    }

    Node current = node;
    while (current != null && !ConsensusUtil.isSameNode(current, ancestor)) {
      String parentHash = current.getParentNodeHash();
      if (parentHash.equals(current.getNodeHash()) || parentHash.equals("0")) {
        return ancestor.getNodeHash().equals(parentHash);
      }
      current = blockTree.get(parentHash);
    }

    return current != null && ConsensusUtil.isSameNode(current, ancestor);
  }

  private void restoreDeferredMessages(ArrayDeque<Message> deferredMessages) {
    while (!deferredMessages.isEmpty()) {
      messageQueue.addFirst(deferredMessages.removeLast());
    }
  }

  private InetSocketAddress requireConsensusEndpoint(long senderId) {
    InetSocketAddress endpoint = consensusEndpointsBySenderId.get(Math.toIntExact(senderId));
    if (endpoint == null) {
      throw new IllegalArgumentException("Unknown consensus endpoint for senderId " + senderId);
    }
    return endpoint;
  }

  private static Map<Integer, InetSocketAddress> buildConsensusEndpointsBySenderId(ConfigParser config) {
    Map<Integer, InetSocketAddress> endpointsBySenderId = new HashMap<>();
    for (ConfigParser.ReplicaSection replica : config.replicas()) {
      try {
        endpointsBySenderId.put(Math.toIntExact(replica.senderId()), new InetSocketAddress(java.net.InetAddress.getByName(replica.host()), replica.consensusPort()));
      } catch (UnknownHostException exception) {
        throw new IllegalStateException("Unable to resolve consensus host for replica " + replica.id(), exception);
      }
    }
    return Map.copyOf(endpointsBySenderId);
  }
}
