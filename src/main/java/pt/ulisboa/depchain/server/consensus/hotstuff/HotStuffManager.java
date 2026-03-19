package pt.ulisboa.depchain.server.consensus.hotstuff;

import java.security.PublicKey;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.weavechain.curve25519.Scalar;

import pt.ulisboa.depchain.proto.AppendNodeCommand;
import pt.ulisboa.depchain.proto.AppendResponse;
import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientResponse;
import pt.ulisboa.depchain.proto.ConsensusMessageType;
import pt.ulisboa.depchain.proto.FetchNodeRequestMessage;
import pt.ulisboa.depchain.proto.FetchNodeResponseMessage;
import pt.ulisboa.depchain.proto.Message;
import pt.ulisboa.depchain.proto.Node;
import pt.ulisboa.depchain.proto.NodeCommand;
import pt.ulisboa.depchain.proto.PhaseCertificateMessage;
import pt.ulisboa.depchain.proto.ProposalMessage;
import pt.ulisboa.depchain.proto.QuorumCertificate;
import pt.ulisboa.depchain.proto.TransactionNodeCommand;
import pt.ulisboa.depchain.proto.TransactionReceipt;
import pt.ulisboa.depchain.proto.TransactionRequest;
import pt.ulisboa.depchain.proto.TransactionResponse;
import pt.ulisboa.depchain.proto.TransactionType;
import pt.ulisboa.depchain.server.consensus.ConsensusTimeoutException;
import pt.ulisboa.depchain.server.consensus.client.ClientRequestManager;
import pt.ulisboa.depchain.server.consensus.network.ReplicaMessenger;
import pt.ulisboa.depchain.server.consensus.threshold.ThresholdSignatureProtocol;
import pt.ulisboa.depchain.server.evm.EvmService;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;
import pt.ulisboa.depchain.shared.utils.TimeUtil;

public class HotStuffManager {
  private static final Wei INITIAL_CLIENT_BALANCE = Wei.of(1_000_000_000L);

  private final ClientRequestManager clientCommunication;
  private final ReplicaMessenger replicaCommunication;
  private final Set<String> executedNodeHashes;
  private final EvmService evmService;
  private final Address clientAccountAddress;

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

  private final HotStuffInbox messageInbox;

  private ThresholdSignatureProtocol thresholdProtocol;
  private final Logger logger;
  private long totalViewChanges;
  private long totalFetchAttempts;
  private long totalFetchFailures;

  public HotStuffManager(int id, ConfigParser config, Scalar localThresholdShare, byte[] publicThresholdKey, PublicKey clientPublicKey, EvmService evmService) {
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
    this.currentProposal = HotStuffSupport.GENESIS_NODE;
    this.viewNumber = 0;
    this.blockTree = new HashMap<>();
    this.clientCommunication = new ClientRequestManager(config.client().senderId(), clientPublicKey, logger);
    this.replicaCommunication = new ReplicaMessenger(id, config, logger);
    this.executedNodeHashes = ConcurrentHashMap.newKeySet();
    this.messageInbox = new HotStuffInbox(id, thresholdProtocol);
    this.evmService = pt.ulisboa.depchain.shared.utils.ValidationUtils.requireNonNull(evmService, "evmService");
    this.clientAccountAddress = clientAddress(clientPublicKey);
    if (this.evmService.account(clientAccountAddress) == null) {
      this.evmService.createAccount(clientAccountAddress, 0L, INITIAL_CLIENT_BALANCE);
    }

    blockTree.put(HotStuffSupport.GENESIS_NODE.getNodeHash(), HotStuffSupport.GENESIS_NODE);
    executedNodeHashes.add(HotStuffSupport.GENESIS_NODE.getNodeHash());
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
      } catch (ConsensusTimeoutException e) {
        onNewView();
      }
    }
  }

  public void onReplicaMessage(Message msg) {
    if (msg.getMessageType() == ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_FETCH_NODE_REQUEST) {
      handleFetchNodeRequest(msg);
      return;
    }

    observeMessage(msg);
    messageInbox.offer(msg);
  }

  public void onClientRequest(ClientRequest request, ConnectionKey key) {
    clientCommunication.onClientRequest(request, key, isLeader());
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
        throw new ConsensusTimeoutException("Timed out waiting for client command");
      }

      NodeCommand command = nodeCommandForRequest(request);
      currentProposal = createLeaf(selectedHighQC.getCertifiedNode().getNodeHash(), command);
      observeNode(currentProposal);

      Message proposal = Message.newBuilder().setViewNumber(viewNumber).setReplicaSenderId(id).setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE)
          .setProposal(ProposalMessage.newBuilder().setProposedNode(currentProposal).setJustifyQc(selectedHighQC)).build();
      broadcast(proposal);
    } else {
      long phaseDeadlineNanos = TimeUtil.monotonicDeadlineAfterNow(viewChangeTimeoutMs);
      Message prepareMsg = messageInbox.waitForMessageFromSenderUntil(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, viewNumber, getLeader(viewNumber), phaseDeadlineNanos);

      QuorumCertificate justify = null;
      Node proposedNode = null;
      if (prepareMsg.hasProposal()) {
        justify = prepareMsg.getProposal().getJustifyQc();
        proposedNode = prepareMsg.getProposal().getProposedNode();
      }

      boolean hasValidJustify = justify != null && verifyQC(justify);
      boolean hasValidProposedNode = proposedNode != null && hasValidNode(proposedNode);
      boolean hasAvailableProposedRequest = proposedNode != null && clientCommunication.observeProposedCommand(proposedNode.getCommand());
      boolean extendsJustifiedNode = justify != null && proposedNode != null && HotStuffSupport.extendsNode(proposedNode, justify.getCertifiedNode());
      boolean isSafeProposal = proposedNode != null && safeNode(proposedNode, justify);
      if (hasValidJustify && hasValidProposedNode && hasAvailableProposedRequest && extendsJustifiedNode && isSafeProposal) {
        observeQuorumCertificate(justify);
        observeNode(proposedNode);
        ensureDeliveredBranch(proposedNode, prepareMsg.getReplicaSenderId(), phaseDeadlineNanos);
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
      long phaseDeadlineNanos = TimeUtil.monotonicDeadlineAfterNow(viewChangeTimeoutMs);
      Message msg = messageInbox.waitForQcMessageUntil(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, viewNumber, getLeader(viewNumber), phaseDeadlineNanos, this::verifyQC);
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
      long phaseDeadlineNanos = TimeUtil.monotonicDeadlineAfterNow(viewChangeTimeoutMs);
      Message msg = messageInbox
          .waitForQcMessageUntil(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT, viewNumber, getLeader(viewNumber), phaseDeadlineNanos, this::verifyQC);
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
      long phaseDeadlineNanos = TimeUtil.monotonicDeadlineAfterNow(viewChangeTimeoutMs);
      Message msg = messageInbox.waitForQcMessageUntil(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_COMMIT, viewNumber, getLeader(viewNumber), phaseDeadlineNanos, this::verifyQC);
      QuorumCertificate decideQC = msg.getPhaseCertificate().getJustifyQc();
      updateHighQC(decideQC);
      ensureDeliveredBranch(decideQC.getCertifiedNode(), msg.getReplicaSenderId(), phaseDeadlineNanos);
      executeCommittedBranch(decideQC.getCertifiedNode());
    }
  }

  private void onNewView() {
    viewNumber++;
    totalViewChanges++;
    int reenqueuedPendingRequests = clientCommunication.enqueuePendingKnownRequestsIfLeader(isLeader());
    if (logger.isDebugEnabled()) {
      logger
          .debug("View change to {}. pendingRequests={}, reenqueuedThisView={}, totalViewChanges={}, totalFetchAttempts={}, totalFetchFailures={}", viewNumber, clientCommunication
              .pendingCount(), reenqueuedPendingRequests, totalViewChanges, totalFetchAttempts, totalFetchFailures);
    }
    sendToLeader(newPhaseCertificateMessage(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_NEW_VIEW, prepareQC));
  }

  private void executeCommand(Node node) {
    NodeCommand command = node.getCommand();
    ClientRequestManager.ExecutionResult executionResult = clientCommunication.markExecuted(node);
    String clientCommand = executionResult.commandSummary();

    if (!HotStuffSupport.isNoOp(command)) {
      logger.debug("Executing command: {}", clientCommand);
    }
    observeNode(node);
    executedNodeHashes.add(node.getNodeHash());
    if (executionResult.replyTarget() == null) {
      return;
    }

    ClientResponse clientResponse = clientResponseForExecution(node, clientCommand);
    clientCommunication.replyToClient(executionResult.replyTarget(), clientResponse);
  }

  private NodeCommand nodeCommandForRequest(ClientRequest request) {
    if (request.hasAppend()) {
      return NodeCommand.newBuilder().setAppend(AppendNodeCommand.newBuilder().setClientRequest(request)).build();
    }
    if (request.hasTransaction()) {
      return NodeCommand.newBuilder().setTransaction(TransactionNodeCommand.newBuilder().setClientRequest(request)).build();
    }
    throw new IllegalArgumentException("ClientRequest body is not set");
  }

  private ClientResponse clientResponseForExecution(Node node, String clientCommand) {
    NodeCommand command = node.getCommand();
    if (command.hasAppend()) {
      return ClientResponse.newBuilder().setAppend(AppendResponse.newBuilder().setSuccess(true).setMessage(successMessage(clientCommand))).build();
    }
    if (command.hasTransaction()) {
      return transactionResponse(node, command.getTransaction().getClientRequest().getTransaction());
    }
    throw new IllegalArgumentException("Node command body is not executable");
  }

  private ClientResponse transactionResponse(Node node, TransactionRequest transaction) {
    String transactionHash = CryptoUtil.sha256Hex(transaction.toByteArray());
    TransactionReceipt.Builder receipt = TransactionReceipt.newBuilder().setTransactionHash(transactionHash).setNodeHash(node.getNodeHash());

    try {
      Address recipient = Address.fromHexString("0x" + transaction.getTo());
      EvmService.TransactionResult execution;
      if (transaction.getType() == TransactionType.TRANSACTION_TYPE_TRANSFER) {
        execution = evmService
            .transferNative(clientAccountAddress, recipient, Wei.of(transaction.getAmount()), transaction.getNonce(), transaction.getGasLimit(), Wei.of(transaction.getGasPrice()));
      } else {
        Bytes callData = Bytes.EMPTY;
        if (transaction.hasData()) {
          callData = Bytes.wrap(transaction.getData().toByteArray());
        }
        execution = evmService.callContract(clientAccountAddress, recipient, callData, Wei.of(transaction.getAmount()), transaction.getNonce(), transaction.getGasLimit(), Wei
            .of(transaction.getGasPrice()));
      }

      receipt.setSuccess(execution.success()).setGasUsed(execution.gasUsed());
      if (execution.errorMessage() != null && !execution.errorMessage().isBlank()) {
        receipt.setErrorMessage(execution.errorMessage());
      }
      if (execution.returnData() != null && !execution.returnData().isEmpty()) {
        receipt.setReturnData(ByteString.copyFrom(execution.returnData().toArrayUnsafe()));
      }

      String message = "Transaction executed successfully";
      if (!execution.success()) {
        message = "Transaction execution failed";
      }
      return ClientResponse.newBuilder().setTransaction(TransactionResponse.newBuilder().setAccepted(true).setMessage(message).setReceipt(receipt)).build();
    } catch (RuntimeException exception) {
      String errorMessage = exception.getMessage();
      if (errorMessage == null || errorMessage.isBlank()) {
        errorMessage = "unexpected transaction execution error";
      }
      receipt.setSuccess(false).setGasUsed(0L).setErrorMessage(errorMessage);
      return ClientResponse.newBuilder().setTransaction(TransactionResponse.newBuilder().setAccepted(true).setMessage("Transaction execution failed").setReceipt(receipt)).build();
    }
  }

  private static String successMessage(String clientCommand) {
    return "Request completed successfully: " + clientCommand;
  }

  private static Address clientAddress(PublicKey clientPublicKey) {
    String publicKeyHash = CryptoUtil.sha256Hex(clientPublicKey.getEncoded());
    return Address.fromHexString("0x" + publicKeyHash.substring(publicKeyHash.length() - 40));
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
    byte[] hashPayload = HotStuffCryptoPayloads.nodeHashPayload(parentHash, viewNumber, command);
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
    if (msg.hasPhaseCertificate()) {
      return msg.getPhaseCertificate().getJustifyQc();
    }
    return null;
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
    if (HotStuffSupport.isSameNode(node, HotStuffSupport.GENESIS_NODE)) {
      return true;
    }

    byte[] expectedHashPayload = HotStuffCryptoPayloads.nodeHashPayload(node.getParentNodeHash(), node.getViewNumber(), node.getCommand());
    return node.getNodeHash().equals(CryptoUtil.sha256Hex(expectedHashPayload));
  }

  private boolean extendsKnownBranch(Node node, Node ancestor) {
    if (node == null || ancestor == null) {
      return false;
    }
    if (HotStuffSupport.isSameNode(node, ancestor)) {
      return true;
    }

    Node current = node;
    while (current != null && !HotStuffSupport.isSameNode(current, ancestor)) {
      String parentHash = current.getParentNodeHash();
      if (parentHash.equals(current.getNodeHash()) || parentHash.equals("0")) {
        return ancestor.getNodeHash().equals(parentHash);
      }
      current = blockTree.get(parentHash);
    }

    return current != null && HotStuffSupport.isSameNode(current, ancestor);
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

  private void ensureDeliveredBranch(Node node, int sourceSenderId, long phaseDeadlineNanos) {
    long deadlineNanos = TimeUtil.boundedMonotonicDeadlineAfterNow(phaseDeadlineNanos, fetchNodeTimeoutMs);
    Node current = node;
    while (current != null && !HotStuffSupport.isSameNode(current, HotStuffSupport.GENESIS_NODE)) {
      observeNode(current);

      String parentHash = current.getParentNodeHash();
      if (parentHash.equals("0")) {
        return;
      }

      Node parentNode = blockTree.get(parentHash);
      if (parentNode != null) {
        if (!isValidParentLink(current, parentNode)) {
          throw new ConsensusTimeoutException("Known ancestor link is invalid for node " + current.getNodeHash());
        }
        current = parentNode;
        continue;
      }

      if (sourceSenderId < 0) {
        throw new ConsensusTimeoutException("Missing ancestor node " + parentHash);
      }

      Node fetchedNode = fetchNodeFromReplicas(sourceSenderId, parentHash, deadlineNanos);
      if (fetchedNode == null) {
        throw new ConsensusTimeoutException("Timed out fetching missing node " + parentHash);
      }
      if (!isValidParentLink(current, fetchedNode)) {
        throw new ConsensusTimeoutException("Fetched ancestor link is invalid for node " + current.getNodeHash());
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
    } catch (ConsensusTimeoutException exception) {
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
    if (HotStuffSupport.isSameNode(parentNode, HotStuffSupport.GENESIS_NODE)) {
      return true;
    }
    return parentNode.getViewNumber() < childNode.getViewNumber();
  }

  private boolean isServableFetchedNode(Node node) {
    if (node == null) {
      return false;
    }
    Node highQcNode = null;
    Node prepareQcNode = null;
    Node lockedQcNode = null;
    if (highQC != null) {
      highQcNode = highQC.getCertifiedNode();
    }
    if (prepareQC != null) {
      prepareQcNode = prepareQC.getCertifiedNode();
    }
    if (lockedQC != null) {
      lockedQcNode = lockedQC.getCertifiedNode();
    }
    return isNodeOnJustifiedBranch(node, currentProposal) || isNodeOnJustifiedBranch(node, highQcNode) || isNodeOnJustifiedBranch(node, prepareQcNode)
        || isNodeOnJustifiedBranch(node, lockedQcNode);
  }

  private boolean isNodeOnJustifiedBranch(Node candidateNode, Node branchHead) {
    if (candidateNode == null || branchHead == null) {
      return false;
    }
    return extendsKnownBranch(branchHead, candidateNode);
  }

}
