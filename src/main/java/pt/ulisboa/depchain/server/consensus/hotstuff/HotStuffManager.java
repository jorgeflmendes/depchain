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
import java.util.function.Consumer;

import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.weavechain.curve25519.Scalar;

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
import pt.ulisboa.depchain.proto.QueryRequest;
import pt.ulisboa.depchain.proto.QueryResponse;
import pt.ulisboa.depchain.proto.QuorumCertificate;
import pt.ulisboa.depchain.proto.TransactionBatchNodeCommand;
import pt.ulisboa.depchain.proto.TransactionReceipt;
import pt.ulisboa.depchain.proto.TransactionRequest;
import pt.ulisboa.depchain.proto.TransactionResponse;
import pt.ulisboa.depchain.proto.TransactionType;
import pt.ulisboa.depchain.server.api.ReplicaClientApi;
import pt.ulisboa.depchain.server.api.ReplicaPeerApi;
import pt.ulisboa.depchain.server.consensus.ConsensusTimeoutException;
import pt.ulisboa.depchain.server.consensus.threshold.ThresholdSignatureProtocol;
import pt.ulisboa.depchain.server.execution.EvmService;
import pt.ulisboa.depchain.server.execution.IstCoin;
import pt.ulisboa.depchain.server.node.BlockStore;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.crypto.CryptoUtil;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.network.links.fairloss.FairLossLink;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.time.TimeUtil;

public class HotStuffManager {
  private static final int MAX_TRANSACTIONS_PER_BLOCK = 32;
  private final ReplicaClientApi clientApi;
  private final ReplicaPeerApi replicaApi;
  private final Set<String> executedNodeHashes;
  private final EvmService evmService;
  private final IstCoin istCoin;
  private final Map<Long, Address> clientAccountAddresses;
  private final Consumer<Node> onNodeExecuted;

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

  public HotStuffManager(int id, ConfigParser config, Scalar localThresholdShare, byte[] publicThresholdKey, Map<Long, PublicKey> clientPublicKeys, EvmService evmService,
      Address istCoinContractAddress, Consumer<Node> onNodeExecuted) {
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
    this.clientApi = new ReplicaClientApi(clientPublicKeys, logger);
    this.replicaApi = new ReplicaPeerApi(id, config, logger);
    this.executedNodeHashes = ConcurrentHashMap.newKeySet();
    this.messageInbox = new HotStuffInbox(id, thresholdProtocol);
    this.evmService = pt.ulisboa.depchain.shared.validation.ValidationUtils.requireNonNull(evmService, "evmService");
    try {
      this.istCoin = istCoinContractAddress == null ? new IstCoin(this.evmService) : new IstCoin(this.evmService, istCoinContractAddress);
    } catch (java.io.IOException exception) {
      throw new IllegalStateException("Could not initialize IST Coin support from genesis", exception);
    }
    this.onNodeExecuted = pt.ulisboa.depchain.shared.validation.ValidationUtils.requireNonNull(onNodeExecuted, "onNodeExecuted");
    this.clientAccountAddresses = clientAddresses(clientPublicKeys);

    blockTree.put(HotStuffSupport.GENESIS_NODE.getNodeHash(), HotStuffSupport.GENESIS_NODE);
    executedNodeHashes.add(HotStuffSupport.GENESIS_NODE.getNodeHash());
  }

  public void initNetwork(AuthenticatedLink nodeTransport, AuthenticatedLink clientTransport) {
    this.replicaApi.bindTransport(nodeTransport);
    this.clientApi.bindTransport(clientTransport);
    this.thresholdProtocol.initTransport(nodeTransport);
  }

  public void run() {
    logger.info("Starting at view {}", viewNumber);
    sendToLeader(createPhaseCertificateMessage(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_NEW_VIEW, prepareQC));

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
    clientApi.registerClientRequest(request, key, isLeader());
  }

  public void onClientQuery(ClientRequest request, ConnectionKey key) {
    if (!request.hasQuery()) {
      throw new IllegalArgumentException("Client query request body is not set");
    }
    if (!clientApi.isValidClientRequest(request)) {
      return;
    }

    clientApi.sendClientResponse(key, ClientResponse.newBuilder().setQuery(queryResponse(request.getQuery())).build());
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
      List<ClientRequest> batch = clientApi.awaitNextPendingBatch(commandDeadlineNanos, BlockStore.MAX_BLOCK_GAS_LIMIT, MAX_TRANSACTIONS_PER_BLOCK);
      if (batch.isEmpty()) {
        throw new ConsensusTimeoutException("Timed out waiting for client command batch");
      }
      batch = fitBatchToProposalTransportBudget(selectedHighQC, batch);
      if (batch.isEmpty()) {
        throw new ConsensusTimeoutException("No pending client command fits the UDP proposal transport budget");
      }

      NodeCommand command = nodeCommandForBatch(batch);
      currentProposal = createLeaf(selectedHighQC.getCertifiedNode().getNodeHash(), command);
      observeNode(currentProposal);

      Message proposal = Message.newBuilder().setViewNumber(viewNumber).setReplicaSenderId(id).setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE)
          .setProposal(ProposalMessage.newBuilder().setProposedNode(currentProposal).setJustifyQc(selectedHighQC)).build();
      replicaApi.broadcastMessage(proposal);
    } else {
      long phaseDeadlineNanos = TimeUtil.monotonicDeadlineAfterNow(viewChangeTimeoutMs);
      Message prepareMsg = messageInbox.waitForMessageFromSenderUntil(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, viewNumber, getLeader(viewNumber), phaseDeadlineNanos);

      QuorumCertificate justify = null;
      Node proposedNode = null;
      if (prepareMsg.hasProposal()) {
        justify = prepareMsg.getProposal().getJustifyQc();
        proposedNode = prepareMsg.getProposal().getProposedNode();
      }

      boolean isValidJustify = justify != null && verifyQC(justify);
      boolean isValidProposedNode = proposedNode != null && isValidNode(proposedNode);
      boolean hasAvailableProposedRequest = proposedNode != null && clientApi.registerProposedCommand(proposedNode.getCommand());
      boolean extendsJustifiedNode = justify != null && proposedNode != null && HotStuffSupport.extendsNode(proposedNode, justify.getCertifiedNode());
      boolean isSafeProposal = proposedNode != null && safeNode(proposedNode, justify);
      if (isValidJustify && isValidProposedNode && hasAvailableProposedRequest && extendsJustifiedNode && isSafeProposal) {
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
      replicaApi.broadcastMessage(createPhaseCertificateMessage(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT, prepareQC));
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
      replicaApi.broadcastMessage(createPhaseCertificateMessage(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_COMMIT, preCommitQC));
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
      replicaApi.broadcastMessage(createPhaseCertificateMessage(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_DECIDE, commitQC));
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
    int reenqueuedPendingRequests = clientApi.enqueuePendingRequestsIfLeader(isLeader());
    if (logger.isDebugEnabled()) {
      logger.debug("View change to {}. pendingRequests={}, reenqueuedThisView={}, totalViewChanges={}, totalFetchAttempts={}, totalFetchFailures={}", viewNumber, clientApi
          .getPendingRequestCount(), reenqueuedPendingRequests, totalViewChanges, totalFetchAttempts, totalFetchFailures);
    }
    sendToLeader(createPhaseCertificateMessage(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_NEW_VIEW, prepareQC));
  }

  private void executeCommand(Node node) {
    NodeCommand command = node.getCommand();
    if (!HotStuffSupport.isNoOp(command)) {
      logger.debug("Executing command: {}", HotStuffSupport.commandSummary(command));
    }
    observeNode(node);
    executedNodeHashes.add(node.getNodeHash());

    long totalGasUsed = 0L;
    List<ReplicaClientApi.ExecutionContext> executionContexts = clientApi.recordExecutedNode(node);
    for (ReplicaClientApi.ExecutionContext executionContext : executionContexts) {
      if (!executionContext.request().hasTransaction()) {
        continue;
      }

      ClientResponse clientResponse = transactionResponse(node, executionContext.request().getTransaction());
      totalGasUsed = Math.addExact(totalGasUsed, gasUsedOf(clientResponse));

      if (executionContext.replyTarget() != null) {
        clientApi.sendClientResponse(executionContext.replyTarget(), clientResponse);
      }
    }

    Node nodeForExecutionHook = node.toBuilder().setGasUsed(totalGasUsed).build();
    try {
      onNodeExecuted.accept(nodeForExecutionHook);
    } catch (RuntimeException exception) {
      logger.error("Node execution hook failed for node {}", node.getNodeHash(), exception);
    }
  }

  private NodeCommand nodeCommandForBatch(List<ClientRequest> requests) {
    pt.ulisboa.depchain.shared.validation.ValidationUtils.requireNonEmpty(requests, "requests");
    return NodeCommand.newBuilder().setTransactionBatch(TransactionBatchNodeCommand.newBuilder().addAllClientRequests(requests)).build();
  }

  private List<ClientRequest> fitBatchToProposalTransportBudget(QuorumCertificate justifyQc, List<ClientRequest> candidateBatch) {
    pt.ulisboa.depchain.shared.validation.ValidationUtils.requireAllNonNull(pt.ulisboa.depchain.shared.validation.ValidationUtils
        .named("justifyQc", justifyQc), pt.ulisboa.depchain.shared.validation.ValidationUtils.named("candidateBatch", candidateBatch));

    ArrayList<ClientRequest> remainingRequests = new ArrayList<>(candidateBatch);
    while (!remainingRequests.isEmpty()) {
      ArrayList<ClientRequest> selectedRequests = new ArrayList<>();
      for (ClientRequest request : remainingRequests) {
        selectedRequests.add(request);
        if (proposalDatagramSize(nodeCommandForBatch(selectedRequests), justifyQc) <= FairLossLink.MAX_PACKET_SIZE) {
          continue;
        }

        selectedRequests.remove(selectedRequests.size() - 1);
        break;
      }

      if (!selectedRequests.isEmpty()) {
        if (selectedRequests.size() < remainingRequests.size()) {
          clientApi.requeuePendingRequests(remainingRequests.subList(selectedRequests.size(), remainingRequests.size()));
        }
        return List.copyOf(selectedRequests);
      }

      ClientRequest oversizedRequest = remainingRequests.remove(0);
      clientApi.discardPendingRequest(oversizedRequest);
      logger.warn("Dropping client request {} because its PREPARE proposal would exceed UDP MAX_PACKET_SIZE ({})", requestSummary(oversizedRequest), FairLossLink.MAX_PACKET_SIZE);
    }

    return List.of();
  }

  private int proposalDatagramSize(NodeCommand command, QuorumCertificate justifyQc) {
    return HotStuffSupport.estimatePrepareProposalDatagramSize(command, justifyQc, viewNumber, id);
  }

  private static String requestSummary(ClientRequest request) {
    if (request == null || !request.hasTransaction()) {
      return "<invalid>";
    }
    TransactionRequest transaction = request.getTransaction();
    return "%d:%d/%s@%d".formatted(transaction.getRequestKey().getClientSenderId(), transaction.getRequestKey().getRequestId(), transaction.getType(), transaction.getGasPrice());
  }

  private static long gasUsedOf(ClientResponse response) {
    if (!response.hasTransaction() || !response.getTransaction().hasReceipt()) {
      return 0L;
    }
    return response.getTransaction().getReceipt().getGasUsed();
  }

  private ClientResponse transactionResponse(Node node, TransactionRequest transaction) {
    String transactionHash = CryptoUtil.sha256Hex(transaction.toByteArray());
    TransactionReceipt.Builder receipt = TransactionReceipt.newBuilder().setTransactionHash(transactionHash).setNodeHash(node.getNodeHash());

    try {
      EvmService.TransactionResult execution = executeTransaction(transaction);

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
      return ClientResponse.newBuilder().setTransaction(TransactionResponse.newBuilder().setMessage(message).setReceipt(receipt)).build();
    } catch (RuntimeException exception) {
      String errorMessage = exception.getMessage();
      if (errorMessage == null || errorMessage.isBlank()) {
        errorMessage = "unexpected transaction execution error";
      }
      receipt.setSuccess(false).setGasUsed(0L).setErrorMessage(errorMessage);
      return ClientResponse.newBuilder().setTransaction(TransactionResponse.newBuilder().setMessage("Transaction execution failed").setReceipt(receipt)).build();
    }
  }

  private EvmService.TransactionResult executeTransaction(TransactionRequest transaction) {
    Address senderAccount = clientAddressFor(transaction.getRequestKey().getClientSenderId());
    Address targetAccount = Address.fromHexString("0x" + transaction.getTo());
    Wei amount = Wei.of(transaction.getAmount());
    Wei gasPrice = Wei.of(transaction.getGasPrice());
    TransactionType type = transaction.getType();

    if (type == TransactionType.TRANSACTION_TYPE_TRANSFER) {
      return evmService.transferNative(senderAccount, targetAccount, amount, transaction.getNonce(), transaction.getGasLimit(), gasPrice);
    }

    if (type == TransactionType.TRANSACTION_TYPE_CONTRACT_CALL) {
      Bytes input = transaction.hasInput() ? Bytes.wrap(transaction.getInput().toByteArray()) : Bytes.EMPTY;
      return evmService.callContract(senderAccount, targetAccount, input, amount, transaction.getNonce(), transaction.getGasLimit(), gasPrice);
    }

    if (type == TransactionType.TRANSACTION_TYPE_IST_COIN_TRANSFER) {
      return istCoin.transfer(senderAccount, targetAccount, transaction.getAmount(), transaction.getNonce(), transaction.getGasLimit(), gasPrice);
    }

    throw new IllegalArgumentException("unsupported transaction type: " + type);
  }

  private QueryResponse queryResponse(QueryRequest query) {
    Address owner = Address.fromHexString("0x" + query.getOwner());

    try {
      EvmService.TransactionResult execution = switch (query.getType()) {
        case QUERY_TYPE_DEPCOIN_BALANCE -> evmService.getNativeBalance(owner);
        case QUERY_TYPE_IST_COIN_BALANCE -> istCoin.getBalance(owner);
        default -> throw new IllegalArgumentException("unsupported query type: " + query.getType());
      };
      QueryResponse.Builder response = QueryResponse.newBuilder().setSuccess(execution.success());
      if (execution.success()) {
        response.setMessage("Query executed successfully");
        if (execution.returnData() != null && !execution.returnData().isEmpty()) {
          response.setReturnData(ByteString.copyFrom(execution.returnData().toArrayUnsafe()));
        }
        return response.build();
      }

      response.setMessage("Query execution failed");
      if (execution.errorMessage() != null && !execution.errorMessage().isBlank()) {
        response.setErrorMessage(execution.errorMessage());
      }
      return response.build();
    } catch (RuntimeException exception) {
      String errorMessage = exception.getMessage();
      if (errorMessage == null || errorMessage.isBlank()) {
        errorMessage = "unexpected query execution error";
      }
      return QueryResponse.newBuilder().setSuccess(false).setMessage("Query execution failed").setErrorMessage(errorMessage).build();
    }
  }

  private Address clientAddressFor(long clientSenderId) {
    Address clientAccountAddress = clientAccountAddresses.get(clientSenderId);
    if (clientAccountAddress == null) {
      throw new IllegalArgumentException("unknown client senderId: " + clientSenderId);
    }
    return clientAccountAddress;
  }

  private static Map<Long, Address> clientAddresses(Map<Long, PublicKey> clientPublicKeys) {
    Map<Long, Address> addressesBySenderId = new HashMap<>();
    for (Map.Entry<Long, PublicKey> entry : clientPublicKeys.entrySet()) {
      addressesBySenderId.put(entry.getKey(), Address.fromHexString("0x" + CryptoUtil.deriveAddressHex(entry.getValue())));
    }
    return Map.copyOf(addressesBySenderId);
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
      replicaApi.sendMessageToReplica(leaderId, msg);
    } catch (Exception e) {
      logger.error("Error sending message to leader", e);
    }
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

  private Message createPhaseCertificateMessage(ConsensusMessageType type, QuorumCertificate justifyQc) {
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
    if (node == null || !isValidNode(node)) {
      return;
    }

    blockTree.putIfAbsent(node.getNodeHash(), node);
  }

  private boolean isValidNode(Node node) {
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

    int[] candidateSenderIds = replicaApi.getCandidateReplicaSenderIds(sourceSenderId);
    Message request = Message.newBuilder().setViewNumber(viewNumber).setReplicaSenderId(id).setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_FETCH_NODE_REQUEST)
        .setFetchNodeRequest(FetchNodeRequestMessage.newBuilder().setNodeHash(nodeHash)).build();
    for (int candidateSenderId : candidateSenderIds) {
      replicaApi.sendMessageToReplica(candidateSenderId, request);
    }
    totalFetchAttempts++;

    ArrayDeque<Message> deferredMessages = new ArrayDeque<>();
    try {
      while (true) {
        Message msg = messageInbox.pollMessageUntil(deadlineNanos, "waiting for missing node " + nodeHash);
        if (msg.getMessageType() == ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_FETCH_NODE_RESPONSE && containsSender(candidateSenderIds, msg.getReplicaSenderId())
            && msg.hasFetchNodeResponse() && nodeHash.equals(msg.getFetchNodeResponse().getNode().getNodeHash())) {
          Node fetchedNode = msg.getFetchNodeResponse().getNode();
          if (!isValidNode(fetchedNode)) {
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

  private static boolean containsSender(int[] candidateSenderIds, int senderId) {
    return Arrays.stream(candidateSenderIds).anyMatch(candidate -> candidate == senderId);
  }

  private void handleFetchNodeRequest(Message msg) {
    if (!msg.hasFetchNodeRequest()) {
      return;
    }

    Node requestedNode = blockTree.get(msg.getFetchNodeRequest().getNodeHash());
    if (requestedNode == null || !canServeFetchedNode(requestedNode)) {
      return;
    }

    Message response = Message.newBuilder().setViewNumber(viewNumber).setReplicaSenderId(id).setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_FETCH_NODE_RESPONSE)
        .setFetchNodeResponse(FetchNodeResponseMessage.newBuilder().setNode(requestedNode)).build();
    replicaApi.sendMessageToReplica(msg.getReplicaSenderId(), response);
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

  private boolean canServeFetchedNode(Node node) {
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
