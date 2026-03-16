package pt.ulisboa.depchain.server.consensus;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.weavechain.curve25519.Scalar;

import pt.ulisboa.depchain.proto.AppendNodeCommand;
import pt.ulisboa.depchain.proto.AppendResponse;
import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientResponse;
import pt.ulisboa.depchain.proto.ConsensusMessageType;
import pt.ulisboa.depchain.proto.FetchNodeRequestMessage;
import pt.ulisboa.depchain.proto.FetchNodeResponseMessage;
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
import pt.ulisboa.depchain.shared.utils.ConsensusPayloadUtil;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;
import pt.ulisboa.depchain.shared.utils.ProtoValidationUtil;
import pt.ulisboa.depchain.shared.utils.TimeUtil;

public class HotStuffManager {
  private final ClientCommunicationManager clientCommunication;
  private final ReplicaCommunicationManager replicaCommunication;
  private final java.util.Set<String> executedNodeHashes;

  private int id;
  private int n;
  private int f;
  private int quorum;
  private long viewChangeTimeoutMs;
  private long clientCommandWaitTimeoutMs;
  private long fetchNodeTimeoutMs;

  private int viewNumber;
  private QuorumCertificate highQC;
  private QuorumCertificate lockedQC;
  private QuorumCertificate prepareQC;
  private Node currentProposal;
  private Map<String, Node> blockTree;

  private final HotStuffMessageInbox messageInbox;

  private ThresholdSignatureProtocol thresholdProtocol;
  private final Logger logger;
  private long totalViewChanges;
  private long totalResubmittedRequests;
  private long totalFetchAttempts;
  private long totalFetchFailures;

  public HotStuffManager(int id, ConfigParser config, Scalar localThresholdShare, byte[] publicThresholdKey, PublicKey clientPublicKey) {
    this.id = id;
    this.logger = LoggerFactory.getLogger(HotStuffManager.class);
    this.n = config.system().n();
    this.f = config.system().f();
    this.quorum = n - f;
    this.viewChangeTimeoutMs = config.timeouts().viewChangeMs();
    this.clientCommandWaitTimeoutMs = config.timeouts().clientCommandWaitMs();
    this.fetchNodeTimeoutMs = config.timeouts().fetchNodeMs();

    this.thresholdProtocol = new ThresholdSignatureProtocol(id, config, localThresholdShare, publicThresholdKey);
    this.highQC = thresholdProtocol.genesisQC();
    this.lockedQC = thresholdProtocol.genesisQC();
    this.prepareQC = thresholdProtocol.genesisQC();
    this.currentProposal = ConsensusUtil.GENESIS_NODE;
    this.viewNumber = 0;
    this.blockTree = new HashMap<>();
    this.clientCommunication = new ClientCommunicationManager(config.client().senderId(), clientPublicKey, logger);
    this.replicaCommunication = new ReplicaCommunicationManager(id, config, logger);
    this.executedNodeHashes = java.util.concurrent.ConcurrentHashMap.newKeySet();
    this.messageInbox = new HotStuffMessageInbox(id, thresholdProtocol);

    blockTree.put(ConsensusUtil.GENESIS_NODE.getNodeHash(), ConsensusUtil.GENESIS_NODE);
    executedNodeHashes.add(ConsensusUtil.GENESIS_NODE.getNodeHash());
  }

  public void initNetwork(AuthenticatedLink nodeTransport, AuthenticatedLink clientTransport) {
    this.replicaCommunication.init(nodeTransport);
    this.clientCommunication.init(clientTransport);
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

  public void onReplicaMessage(Message msg) {
    if (msg.getMessageType() == ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_FORWARDED_REQUEST) {
      clientCommunication.receiveForwardedRequest(msg.hasForwardedRequest() ? msg.getForwardedRequest().getClientRequest() : null, isLeader());
      return;
    }

    if (msg.getMessageType() == ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_FETCH_NODE_REQUEST) {
      handleFetchNodeRequest(msg);
      return;
    }

    observeMessage(msg);
    messageInbox.offer(msg);
  }

  public void onClientRequest(ClientRequest request, ConnectionKey key) {
    clientCommunication.receiveClientRequest(request, key, isLeader(), this::forwardClientRequestToLeader);
  }

  private void forwardClientRequestToLeader(ClientRequest request) {
    if (request == null || !request.hasAppend()) {
      return;
    }

    Message forwardedRequest = Message.newBuilder().setViewNumber(viewNumber).setReplicaSenderId(id)
        .setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_FORWARDED_REQUEST)
        .setForwardedRequest(ForwardedRequestMessage.newBuilder().setClientRequest(request)).build();
    sendToLeader(forwardedRequest);
  }

  private void onPrepare() {
    if (isLeader()) {
      long viewDeadlineNanos = TimeUtil.monotonicDeadlineAfterNow(viewChangeTimeoutMs);
      List<Message> msgs = messageInbox.waitForValidNewViewsUntil(viewNumber, quorum - 1, viewDeadlineNanos, this::verifyQC);
      QuorumCertificate selectedHighQC = highQC;
      for (Message m : msgs) {
        QuorumCertificate justifyQc = justifyQCOrNull(m);
        if (justifyQc != null && justifyQc.getViewNumber() > selectedHighQC.getViewNumber()) {
          selectedHighQC = justifyQc;
        }
      }
      updateHighQC(selectedHighQC);

      long commandDeadlineNanos = TimeUtil.boundedMonotonicDeadlineAfterNow(viewDeadlineNanos, clientCommandWaitTimeoutMs);
      ClientRequest request = clientCommunication.awaitNextPending(commandDeadlineNanos);
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
      Message prepareMsg = messageInbox.waitForMessageFromSender(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, viewNumber, getLeader(viewNumber), viewChangeTimeoutMs);
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
        ensureDeliveredBranchOnPrepare(proposedNode, prepareMsg.getReplicaSenderId());
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
      Message msg = messageInbox.waitForQcMessage(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, viewNumber, getLeader(viewNumber), viewChangeTimeoutMs, this::verifyQC);
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
      Message msg = messageInbox.waitForQcMessage(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT, viewNumber, getLeader(viewNumber), viewChangeTimeoutMs, this::verifyQC);
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
      executeCommittedBranch(currentProposal);
    } else {
      Message msg = messageInbox.waitForQcMessage(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_COMMIT, viewNumber, getLeader(viewNumber), viewChangeTimeoutMs, this::verifyQC);
      QuorumCertificate decideQC = msg.getPhaseCertificate().getJustifyQc();
      updateHighQC(decideQC);
      ensureDeliveredBranchOnDecide(decideQC.getCertifiedNode(), msg.getReplicaSenderId());
      executeCommittedBranch(decideQC.getCertifiedNode());
    }
  }

  private void onNewView() {
    viewNumber++;
    totalViewChanges++;
    int resubmittedRequests = clientCommunication.resubmitPendingRequests(isLeader(), this::forwardClientRequestToLeader);
    if (logger.isDebugEnabled()) {
      logger.debug("View change to {}. pendingRequests={}, enqueuedThisView={}, totalViewChanges={}, totalFetchAttempts={}, totalFetchFailures={}",
          viewNumber, clientCommunication.pendingCount(), resubmittedRequests, totalViewChanges, totalFetchAttempts, totalFetchFailures);
    }
    totalResubmittedRequests += resubmittedRequests;
    sendToLeader(newPhaseCertificateMessage(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_NEW_VIEW, prepareQC));
  }

  private void executeCommand(Node node) {
    NodeCommand command = node.getCommand();
    ClientCommunicationManager.ExecutionResult executionResult = clientCommunication.markExecuted(node);
    String clientCommand = executionResult.commandValue();

    if (!ConsensusUtil.isNoOp(command)) {
      logger.debug("Executing command: {}", clientCommand);
    }
    observeNode(node);
    executedNodeHashes.add(node.getNodeHash());
    if (executionResult.replyTarget() == null) {
      return;
    }

    ClientResponse clientResponse = ClientResponse.newBuilder().setAppend(AppendResponse.newBuilder().setSuccess(true).setMessage("Success: " + clientCommand)).build();
    clientCommunication.replyToClient(executionResult.replyTarget(), clientResponse);
  }

  private void broadcast(Message msg) {
    replicaCommunication.broadcast(msg);
  }

  private void sendToLeader(Message msg) {
    try {
      int leaderId = getLeader(viewNumber);
      if (leaderId == id) {
        if (msg.getMessageType() != ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_NEW_VIEW) {
          onReplicaMessage(msg);
        }
        return;
      }
      replicaCommunication.sendToReplica(leaderId, msg);
    } catch (Exception e) {
      logger.error("Error sending message to leader", e);
    }
  }

  private void sendToReplica(int replicaSenderId, Message msg) {
    replicaCommunication.sendToReplica(replicaSenderId, msg);
  }

  private Message voteMessage(ConsensusMessageType type, Node node) {
    return thresholdProtocol.createVoteMessage(viewNumber, type, node, getLeader(viewNumber), messageInbox.sharedQueueForThreshold());
  }

  private QuorumCertificate buildQC(ConsensusMessageType type, Node node) {
    return thresholdProtocol.buildQC(viewNumber, type, node, messageInbox.sharedQueueForThreshold());
  }

  private boolean verifyQC(QuorumCertificate qc) {
    return thresholdProtocol.verifyQC(qc);
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

  private boolean isLeader() {
    return getLeader(viewNumber) == id;
  }

  private Message newPhaseCertificateMessage(ConsensusMessageType type, QuorumCertificate justifyQc) {
    return Message.newBuilder().setViewNumber(viewNumber).setReplicaSenderId(id).setMessageType(type)
        .setPhaseCertificate(PhaseCertificateMessage.newBuilder().setJustifyQc(justifyQc)).build();
  }

  private QuorumCertificate justifyQCOrNull(Message msg) {
    return msg.hasPhaseCertificate() ? msg.getPhaseCertificate().getJustifyQc() : null;
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

  private void executeCommittedBranch(Node decidedNode) {
    List<Node> branchToExecute = new ArrayList<>();
    Node current = decidedNode;
    while (current != null && !executedNodeHashes.contains(current.getNodeHash())) {
      branchToExecute.add(current);
      if (current.getParentNodeHash().equals("0")) {
        current = null;
      } else {
        current = blockTree.get(current.getParentNodeHash());
      }
    }

    for (int i = branchToExecute.size() - 1; i >= 0; i--) {
      executeCommand(branchToExecute.get(i));
    }
  }

  private void ensureDeliveredBranchOnPrepare(Node proposedNode, int sourceSenderId) {
    ensureDeliveredBranch(proposedNode, sourceSenderId);
  }

  private void ensureDeliveredBranchOnDecide(Node decidedNode, int sourceSenderId) {
    ensureDeliveredBranch(decidedNode, sourceSenderId);
  }

  private void ensureDeliveredBranch(Node targetNode, int sourceSenderId) {
    long deadlineNanos = TimeUtil.boundedMonotonicDeadlineAfterNow(TimeUtil.monotonicDeadlineAfterNow(viewChangeTimeoutMs), fetchNodeTimeoutMs);
    ensureDeliveredBranchWithinDeadline(targetNode, sourceSenderId, deadlineNanos);
  }

  private void ensureDeliveredBranchWithinDeadline(Node node, int sourceSenderId, long deadlineNanos) {
    Node current = node;
    while (current != null && !ConsensusUtil.isSameNode(current, ConsensusUtil.GENESIS_NODE)) {
      observeNode(current);

      String parentHash = current.getParentNodeHash();
      if (parentHash.equals("0")) {
        return;
      }

      Node parentNode = blockTree.get(parentHash);
      if (parentNode != null) {
        if (!isValidParentLink(current, parentNode)) {
          throw new ViewChangeTimeoutException("Known ancestor link is invalid for node " + current.getNodeHash());
        }
        current = parentNode;
        continue;
      }

      if (sourceSenderId < 0) {
        throw new ViewChangeTimeoutException("Missing ancestor node " + parentHash);
      }

      Node fetchedNode = fetchNodeFromReplicas(sourceSenderId, parentHash, deadlineNanos);
      if (fetchedNode == null) {
        throw new ViewChangeTimeoutException("Timed out fetching missing node " + parentHash);
      }
      if (!isValidParentLink(current, fetchedNode)) {
        throw new ViewChangeTimeoutException("Fetched ancestor link is invalid for node " + current.getNodeHash());
      }
      current = fetchedNode;
    }
  }

  private Node fetchNodeFromReplicas(int sourceSenderId, String nodeHash, long deadlineNanos) {
    Node knownNode = blockTree.get(nodeHash);
    if (knownNode != null) {
      return knownNode;
    }

    int[] candidateSenderIds = candidateFetchSources(sourceSenderId);
    Message request = Message.newBuilder().setViewNumber(viewNumber).setReplicaSenderId(id).setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_FETCH_NODE_REQUEST)
        .setFetchNodeRequest(FetchNodeRequestMessage.newBuilder().setNodeHash(nodeHash)).build();
    for (int candidateSenderId : candidateSenderIds) {
      sendToReplica(candidateSenderId, request);
    }
    totalFetchAttempts++;

    ArrayDeque<Message> deferredMessages = new ArrayDeque<>();
    try {
      while (true) {
        Message msg = messageInbox.pollMessageUntil(deadlineNanos, "waiting for missing node " + nodeHash);
        if (msg.getMessageType() == ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_FETCH_NODE_RESPONSE && containsSender(candidateSenderIds, msg.getReplicaSenderId())
            && msg.hasFetchNodeResponse() && nodeHash.equals(msg.getFetchNodeResponse().getNode().getNodeHash())) {
          Node fetchedNode = msg.getFetchNodeResponse().getNode();
          if (!hasValidNode(fetchedNode)) {
            deferredMessages.addLast(msg);
            continue;
          }
          observeNode(fetchedNode);
          return fetchedNode;
        }
        deferredMessages.addLast(msg);
      }
    } catch (ViewChangeTimeoutException exception) {
      totalFetchFailures++;
      throw exception;
    } finally {
      messageInbox.restoreDeferredMessages(deferredMessages);
    }
  }

  private int[] candidateFetchSources(int preferredSourceSenderId) {
    return replicaCommunication.candidateReplicaSenderIds(preferredSourceSenderId);
  }

  private static boolean containsSender(int[] candidateSenderIds, int senderId) {
    return Arrays.stream(candidateSenderIds).anyMatch(candidate -> candidate == senderId);
  }

  private void handleFetchNodeRequest(Message msg) {
    if (!msg.hasFetchNodeRequest()) {
      return;
    }

    Node requestedNode = blockTree.get(msg.getFetchNodeRequest().getNodeHash());
    if (requestedNode == null || !isServableFetchedNode(requestedNode)) {
      return;
    }

    Message response = Message.newBuilder().setViewNumber(viewNumber).setReplicaSenderId(id).setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_FETCH_NODE_RESPONSE)
        .setFetchNodeResponse(FetchNodeResponseMessage.newBuilder().setNode(requestedNode)).build();
    sendToReplica(msg.getReplicaSenderId(), response);
  }

  private boolean isValidParentLink(Node childNode, Node parentNode) {
    if (childNode == null || parentNode == null) {
      return false;
    }
    if (!childNode.getParentNodeHash().equals(parentNode.getNodeHash())) {
      return false;
    }
    if (ConsensusUtil.isSameNode(parentNode, ConsensusUtil.GENESIS_NODE)) {
      return true;
    }
    return parentNode.getViewNumber() < childNode.getViewNumber();
  }

  private boolean isServableFetchedNode(Node node) {
    if (node == null) {
      return false;
    }
    return isNodeOnJustifiedBranch(node, currentProposal)
        || isNodeOnJustifiedBranch(node, highQC != null ? highQC.getCertifiedNode() : null)
        || isNodeOnJustifiedBranch(node, prepareQC != null ? prepareQC.getCertifiedNode() : null)
        || isNodeOnJustifiedBranch(node, lockedQC != null ? lockedQC.getCertifiedNode() : null);
  }

  private boolean isNodeOnJustifiedBranch(Node candidateNode, Node branchHead) {
    if (candidateNode == null || branchHead == null) {
      return false;
    }
    return extendsKnownBranch(branchHead, candidateNode);
  }

}
