package pt.ulisboa.depchain.integration.byzantine;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.ClientResponse;
import pt.ulisboa.depchain.proto.ConsensusMessageType;
import pt.ulisboa.depchain.proto.Message;
import pt.ulisboa.depchain.proto.Node;
import pt.ulisboa.depchain.proto.NodeCommand;
import pt.ulisboa.depchain.proto.PhaseCertificateMessage;
import pt.ulisboa.depchain.proto.ProposalMessage;
import pt.ulisboa.depchain.proto.QuorumCertificate;
import pt.ulisboa.depchain.proto.TransactionBatchNodeCommand;
import pt.ulisboa.depchain.proto.TransactionReceipt;
import pt.ulisboa.depchain.proto.TransactionRequest;
import pt.ulisboa.depchain.proto.TransactionResponse;
import pt.ulisboa.depchain.proto.TransactionType;
import pt.ulisboa.depchain.proto.VoteMessage;
import pt.ulisboa.depchain.server.consensus.hotstuff.HotStuffCryptoPayloads;
import pt.ulisboa.depchain.server.consensus.hotstuff.HotStuffSupport;
import pt.ulisboa.depchain.server.consensus.network.ReplicaTransportIds;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.crypto.ClientRequestSignaturePayloadUtil;
import pt.ulisboa.depchain.shared.crypto.CryptoUtil;
import pt.ulisboa.depchain.shared.crypto.key.PrivateKeyLoader;
import pt.ulisboa.depchain.shared.crypto.key.PublicKeyLoader;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.validation.ProtoValidationUtil;

public final class ByzantineReplicaNode {
  public static final String ATTACK_MARKER = "Byzantine attack observed:";
  private static final String TEST_RECIPIENT_ADDRESS = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

  private static final Logger logger = LoggerFactory.getLogger(ByzantineReplicaNode.class);

  private final ConfigParser configParser;
  private final ConfigParser.ReplicaSection replicaConfig;
  private final ByzantineAttackMode attackMode;
  private final PrivateKey localStaticSKey;
  private final Map<Long, PublicKey> clientStaticPKeys;
  private final Map<Long, PublicKey> replicaStaticPKeys;
  private final Map<String, Long> replicaSenderIdByConsensusEndpoint;
  private final Map<Integer, InetSocketAddress> consensusEndpointsBySenderId;
  private final AtomicBoolean attackObserved;
  private final AtomicBoolean sentAttackMessage;

  public ByzantineReplicaNode(String replicaId, String configPath, ByzantineAttackMode attackMode) throws Exception {
    this.configParser = ConfigParser.load(Path.of(configPath));
    this.replicaConfig = configParser.requireReplicaById(replicaId);
    this.attackMode = attackMode;
    this.localStaticSKey = PrivateKeyLoader.loadReplicaPrivateKey(configParser, replicaConfig.senderId());
    this.clientStaticPKeys = PublicKeyLoader.loadClientPublicKeys(configParser);
    this.replicaStaticPKeys = PublicKeyLoader.loadReplicaPublicKeys(configParser);
    this.replicaSenderIdByConsensusEndpoint = buildReplicaSenderIdByConsensusEndpoint(configParser);
    this.consensusEndpointsBySenderId = buildConsensusEndpointsBySenderId(configParser);
    this.attackObserved = new AtomicBoolean();
    this.sentAttackMessage = new AtomicBoolean();
  }

  public void run() throws Exception {
    InetSocketAddress clientBindEndpoint = new InetSocketAddress(InetAddress.getByName(replicaConfig.host()), replicaConfig.clientPort());
    InetSocketAddress nodeBindEndpoint = new InetSocketAddress(InetAddress.getByName(replicaConfig.host()), replicaConfig.consensusPort());
    ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();

    try (workers;
        AuthenticatedLink clientTransport = AuthenticatedLink.bind(clientBindEndpoint, replicaConfig.senderId(), localStaticSKey, clientStaticPKeys);
        AuthenticatedLink nodeTransport = AuthenticatedLink.bind(nodeBindEndpoint, replicaConfig.senderId(), localStaticSKey, replicaStaticPKeys)) {

      logger.info("Replica {} client listener: {}:{}", replicaConfig.id(), replicaConfig.host(), replicaConfig.clientPort());
      logger.info("Replica {} node listener: {}:{}", replicaConfig.id(), replicaConfig.host(), replicaConfig.consensusPort());

      sendInitialAttackMessage(nodeTransport);
      workers.submit(() -> runNodeLoop(nodeTransport));
      runClientLoop(clientTransport);
    }
  }

  private void runClientLoop(AuthenticatedLink transport) {
    while (true) {
      InboundPacket request = receiveNextInbound(transport);
      if (request == null) {
        continue;
      }

      handleClientRequest(transport, request);
    }
  }

  private void runNodeLoop(AuthenticatedLink transport) {
    while (true) {
      InboundPacket request = receiveNextInbound(transport);
      if (request == null) {
        continue;
      }

      handleNodeRequest(transport, request);
    }
  }

  private InboundPacket receiveNextInbound(AuthenticatedLink transport) {
    try {
      return transport.receive();
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      return null;
    } catch (Exception exception) {
      return null;
    }
  }

  private void handleClientRequest(AuthenticatedLink clientTransport, InboundPacket inbound) {
    if (inbound.authenticatedSenderId() == null || !clientStaticPKeys.containsKey(inbound.authenticatedSenderId())) {
      return;
    }

    try {
      ClientRequest request = ProtoValidationUtil.requireValid(ClientRequest.parseFrom(inbound.payload()), "ClientRequest");
      switch (attackMode) {
        case FORGED_CLIENT_SUCCESS_RESPONSE -> maybeSendForgedClientSuccessResponse(clientTransport, inbound, request);
        case FORGED_CLIENT_FAILURE_RESPONSE -> maybeSendForgedClientFailureResponse(clientTransport, inbound, request);
        default -> observeAttack();
      }
    } catch (Exception ignored) {
    }
  }

  private void handleNodeRequest(AuthenticatedLink nodeTransport, InboundPacket inbound) {
    InetSocketAddress sender = inbound.sender();
    try {
      Long expectedSenderId = replicaSenderIdByConsensusEndpoint.get(endpointKey(sender));
      if (expectedSenderId == null || inbound.authenticatedSenderId() == null || !expectedSenderId.equals(inbound.authenticatedSenderId())) {
        return;
      }

      Message message = ProtoValidationUtil.requireValid(Message.parseFrom(inbound.payload()), "ReplicaMessage");
      if (message.getReplicaSenderId() != expectedSenderId.intValue()) {
        return;
      }

      switch (attackMode) {
        case DROP_ALL_MESSAGES -> observeAttack();
        case INVALID_NEW_VIEW -> observeAttack();
        case SPOOFED_REPLICA_SENDER_ID -> maybeSendSpoofedSenderVote(nodeTransport, inbound, message);
        case INVALID_PREPARE_VOTE -> maybeSendInvalidVote(nodeTransport, inbound, message, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, false);
        case INVALID_PRE_COMMIT_VOTE -> maybeSendInvalidVote(nodeTransport, inbound, message, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT, false);
        case INVALID_COMMIT_VOTE -> maybeSendInvalidVote(nodeTransport, inbound, message, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_COMMIT, false);
        case STALE_PREPARE_VOTE -> maybeSendInvalidVote(nodeTransport, inbound, message, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, true);
        case STALE_PRE_COMMIT_VOTE -> maybeSendInvalidVote(nodeTransport, inbound, message, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT, true);
        case STALE_COMMIT_VOTE -> maybeSendInvalidVote(nodeTransport, inbound, message, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_COMMIT, true);
        case INVALID_PREPARE_PROPOSAL_QC -> maybeBroadcastInvalidLeaderMessage(nodeTransport, message, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE);
        case INVALID_PRE_COMMIT_QC -> maybeBroadcastInvalidLeaderMessage(nodeTransport, message, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT);
        case INVALID_COMMIT_QC -> maybeBroadcastInvalidLeaderMessage(nodeTransport, message, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_COMMIT);
        case INVALID_DECIDE_QC -> maybeBroadcastInvalidLeaderMessage(nodeTransport, message, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_DECIDE);
        case EQUIVOCATING_PREPARE_PROPOSAL -> maybeBroadcastEquivocatingPrepare(nodeTransport, message);
        case PARTIAL_PREPARE_BROADCAST -> maybeBroadcastPartialPrepare(nodeTransport, message);
        case PARTIAL_PRE_COMMIT_BROADCAST -> maybeBroadcastPartialPhaseCertificate(nodeTransport, message, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT);
        case PARTIAL_COMMIT_BROADCAST -> maybeBroadcastPartialPhaseCertificate(nodeTransport, message, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_COMMIT);
        case PARTIAL_DECIDE_BROADCAST -> maybeBroadcastPartialPhaseCertificate(nodeTransport, message, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_DECIDE);
      }
    } catch (Exception ignored) {
    }
  }

  private void sendInitialAttackMessage(AuthenticatedLink nodeTransport) throws Exception {
    if (isInitialLeader()) {
      return;
    }

    if (attackMode == ByzantineAttackMode.DROP_ALL_MESSAGES) {
      observeAttack();
      return;
    }

    sendInitialNewView(nodeTransport);
  }

  private void maybeSendInvalidVote(AuthenticatedLink nodeTransport, InboundPacket inbound, Message message, ConsensusMessageType expectedType, boolean staleView)
      throws Exception {
    if (sentAttackMessage.get() || message.getMessageType() != expectedType) {
      return;
    }

    Node originalNode = attackedNode(message);
    if (originalNode == null) {
      return;
    }

    int voteView = message.getViewNumber();
    if (staleView) {
      voteView = Math.max(0, voteView - 1);
    }

    Node votedNode = originalNode;
    if (!staleView) {
      votedNode = originalNode.toBuilder().setNodeHash(invalidSha256Hex(originalNode.getNodeHash())).build();
    }
    Message invalidVote = Message.newBuilder().setViewNumber(voteView).setReplicaSenderId(Math.toIntExact(replicaConfig.senderId())).setMessageType(expectedType)
        .setVote(VoteMessage.newBuilder().setVotedNode(votedNode).setThresholdSignatureShare(ByteString.copyFrom(new byte[32]))
            .setAggregatedCommitment(ByteString.copyFrom(new byte[32])))
        .build();
    nodeTransport.send(inbound.packet().getConnectionId(), ProtoValidationUtil.requireValid(invalidVote, "ReplicaMessage").toByteArray(), inbound.sender());
    observeAttack();
    sentAttackMessage.set(true);
  }

  private void maybeSendSpoofedSenderVote(AuthenticatedLink nodeTransport, InboundPacket inbound, Message message) throws Exception {
    if (sentAttackMessage.get() || !isVoteTrigger(message)) {
      return;
    }

    Node originalNode = attackedNode(message);
    if (originalNode == null) {
      return;
    }

    Message spoofedVote = Message.newBuilder().setViewNumber(message.getViewNumber()).setReplicaSenderId(1).setMessageType(message.getMessageType()).setVote(VoteMessage
        .newBuilder().setVotedNode(originalNode).setThresholdSignatureShare(ByteString.copyFrom(new byte[32])).setAggregatedCommitment(ByteString.copyFrom(new byte[32]))).build();
    nodeTransport.send(inbound.packet().getConnectionId(), ProtoValidationUtil.requireValid(spoofedVote, "ReplicaMessage").toByteArray(), inbound.sender());
    observeAttack();
    sentAttackMessage.set(true);
  }

  private void maybeSendForgedClientSuccessResponse(AuthenticatedLink clientTransport, InboundPacket inbound, ClientRequest request) throws Exception {
    if (sentAttackMessage.get() || !request.hasTransaction()) {
      return;
    }

    String fakeHash = CryptoUtil.sha256Hex(("forged-client-success-" + request.getTransaction().getRequestKey().getRequestId()).getBytes(java.nio.charset.StandardCharsets.UTF_8));
    ClientResponse response = ClientResponse.newBuilder().setTransaction(TransactionResponse.newBuilder().setMessage("Transaction executed successfully")
        .setReceipt(TransactionReceipt.newBuilder().setSuccess(true).setGasUsed(21_000L).setTransactionHash(fakeHash).setNodeHash(fakeHash))).build();
    clientTransport.send(inbound.packet().getConnectionId(), ProtoValidationUtil.requireValid(response, "ClientResponse").toByteArray(), inbound.sender());
    observeAttack();
    sentAttackMessage.set(true);
  }

  private void maybeSendForgedClientFailureResponse(AuthenticatedLink clientTransport, InboundPacket inbound, ClientRequest request) throws Exception {
    if (sentAttackMessage.get() || !request.hasTransaction()) {
      return;
    }

    String fakeHash = CryptoUtil.sha256Hex(("forged-client-failure-" + request.getTransaction().getRequestKey().getRequestId()).getBytes(java.nio.charset.StandardCharsets.UTF_8));
    ClientResponse response = ClientResponse.newBuilder().setTransaction(TransactionResponse.newBuilder().setMessage("Transaction execution failed").setReceipt(TransactionReceipt
        .newBuilder().setSuccess(false).setGasUsed(0L).setTransactionHash(fakeHash).setNodeHash(fakeHash).setErrorMessage("byzantine forged failure"))).build();
    clientTransport.send(inbound.packet().getConnectionId(), ProtoValidationUtil.requireValid(response, "ClientResponse").toByteArray(), inbound.sender());
    observeAttack();
    sentAttackMessage.set(true);
  }

  private void maybeBroadcastInvalidLeaderMessage(AuthenticatedLink nodeTransport, Message message, ConsensusMessageType maliciousType) throws Exception {
    if (sentAttackMessage.get() || message.getMessageType() != ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_NEW_VIEW) {
      return;
    }

    Message maliciousMessage = switch (maliciousType) {
      case CONSENSUS_MESSAGE_TYPE_PREPARE -> invalidPrepareProposal();
      case CONSENSUS_MESSAGE_TYPE_PRE_COMMIT, CONSENSUS_MESSAGE_TYPE_COMMIT, CONSENSUS_MESSAGE_TYPE_DECIDE -> invalidPhaseCertificate(maliciousType);
      default -> null;
    };
    if (maliciousMessage == null) {
      return;
    }

    byte[] payload = ProtoValidationUtil.requireValid(maliciousMessage, "ReplicaMessage").toByteArray();
    int localSenderId = Math.toIntExact(replicaConfig.senderId());
    for (Map.Entry<Integer, InetSocketAddress> entry : consensusEndpointsBySenderId.entrySet()) {
      if (entry.getKey() == localSenderId) {
        continue;
      }
      nodeTransport.send(ReplicaTransportIds
          .connectionIdForView(maliciousMessage.getViewNumber(), localSenderId, entry.getKey(), ReplicaTransportIds.REPLICA_MESSAGE_LANE), payload, entry.getValue());
    }
    observeAttack();
    sentAttackMessage.set(true);
  }

  private void maybeBroadcastEquivocatingPrepare(AuthenticatedLink nodeTransport, Message message) throws Exception {
    if (sentAttackMessage.get() || message.getMessageType() != ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_NEW_VIEW) {
      return;
    }

    Message firstPrepare = validPrepareProposal(0, 10_001L, "equivocation-a");
    Message secondPrepare = validPrepareProposal(0, 10_002L, "equivocation-b");

    int localSenderId = Math.toIntExact(replicaConfig.senderId());
    boolean sentFirst = false;
    for (Map.Entry<Integer, InetSocketAddress> entry : consensusEndpointsBySenderId.entrySet()) {
      if (entry.getKey() == localSenderId) {
        continue;
      }

      Message proposal = sentFirst ? secondPrepare : firstPrepare;
      sentFirst = true;
      nodeTransport
          .send(ReplicaTransportIds.connectionIdForView(proposal.getViewNumber(), localSenderId, entry.getKey(), ReplicaTransportIds.REPLICA_MESSAGE_LANE), ProtoValidationUtil
              .requireValid(proposal, "ReplicaMessage").toByteArray(), entry.getValue());
    }

    observeAttack();
    sentAttackMessage.set(true);
  }

  private void maybeBroadcastPartialPrepare(AuthenticatedLink nodeTransport, Message message) throws Exception {
    if (sentAttackMessage.get() || message.getMessageType() != ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_NEW_VIEW) {
      return;
    }

    Message prepare = validPrepareProposal(0, 10_101L, "partial-prepare");
    int localSenderId = Math.toIntExact(replicaConfig.senderId());
    for (Map.Entry<Integer, InetSocketAddress> entry : consensusEndpointsBySenderId.entrySet()) {
      if (entry.getKey() == localSenderId) {
        continue;
      }

      nodeTransport
          .send(ReplicaTransportIds.connectionIdForView(prepare.getViewNumber(), localSenderId, entry.getKey(), ReplicaTransportIds.REPLICA_MESSAGE_LANE), ProtoValidationUtil
              .requireValid(prepare, "ReplicaMessage").toByteArray(), entry.getValue());
      break;
    }

    observeAttack();
    sentAttackMessage.set(true);
  }

  private void maybeBroadcastPartialPhaseCertificate(AuthenticatedLink nodeTransport, Message message, ConsensusMessageType maliciousType) throws Exception {
    if (sentAttackMessage.get() || message.getMessageType() != ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_NEW_VIEW) {
      return;
    }

    Message maliciousMessage = invalidPhaseCertificate(maliciousType);
    int localSenderId = Math.toIntExact(replicaConfig.senderId());
    for (Map.Entry<Integer, InetSocketAddress> entry : consensusEndpointsBySenderId.entrySet()) {
      if (entry.getKey() == localSenderId) {
        continue;
      }

      nodeTransport.send(ReplicaTransportIds.connectionIdForView(maliciousMessage.getViewNumber(), localSenderId, entry
          .getKey(), ReplicaTransportIds.REPLICA_MESSAGE_LANE), ProtoValidationUtil.requireValid(maliciousMessage, "ReplicaMessage").toByteArray(), entry.getValue());
      break;
    }

    observeAttack();
    sentAttackMessage.set(true);
  }

  private Message invalidPrepareProposal() {
    Node proposedNode = validLeafNode(0, 0L, "invalid-proposal");
    QuorumCertificate invalidJustifyQc = invalidQc(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 0, HotStuffSupport.GENESIS_NODE);
    return Message.newBuilder().setViewNumber(0).setReplicaSenderId(Math.toIntExact(replicaConfig.senderId())).setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE)
        .setProposal(ProposalMessage.newBuilder().setProposedNode(proposedNode).setJustifyQc(invalidJustifyQc)).build();
  }

  private Message validPrepareProposal(int view, long requestId, String value) {
    Node proposedNode = validLeafNode(view, requestId, value);
    return Message.newBuilder().setViewNumber(view).setReplicaSenderId(Math.toIntExact(replicaConfig.senderId()))
        .setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE).setProposal(ProposalMessage.newBuilder().setProposedNode(proposedNode).setJustifyQc(validGenesisQc()))
        .build();
  }

  private Message invalidPhaseCertificate(ConsensusMessageType maliciousType) {
    ConsensusMessageType qcType = switch (maliciousType) {
      case CONSENSUS_MESSAGE_TYPE_PRE_COMMIT -> ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE;
      case CONSENSUS_MESSAGE_TYPE_COMMIT -> ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT;
      case CONSENSUS_MESSAGE_TYPE_DECIDE -> ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_COMMIT;
      default -> throw new IllegalArgumentException("Unsupported malicious phase type " + maliciousType);
    };
    QuorumCertificate invalidJustifyQc = invalidQc(qcType, 0, validLeafNode(0, 1L, "invalid-qc-node"));
    return Message.newBuilder().setViewNumber(0).setReplicaSenderId(Math.toIntExact(replicaConfig.senderId())).setMessageType(maliciousType)
        .setPhaseCertificate(PhaseCertificateMessage.newBuilder().setJustifyQc(invalidJustifyQc)).build();
  }

  private QuorumCertificate invalidQc(ConsensusMessageType qcType, int qcView, Node certifiedNode) {
    return QuorumCertificate.newBuilder().setMessageType(qcType).setViewNumber(qcView).setCertifiedNode(certifiedNode).setQuorumSignature(ByteString.copyFrom(new byte[64]))
        .build();
  }

  private Node validLeafNode(int view, long requestId, String value) {
    try {
      long clientSenderId = configParser.client().senderId();
      PrivateKey clientPrivateKey = PrivateKeyLoader.loadClientPrivateKey(configParser);
      long nonce = 1_000_000L + requestId;
      byte[] signature = CryptoUtil.signEcdsa(ClientRequestSignaturePayloadUtil
          .signedTransactionRequestPayload(clientSenderId, requestId, TransactionType.TRANSACTION_TYPE_TRANSFER, TEST_RECIPIENT_ADDRESS, requestId
              + 1, nonce, 21_000L, 1L), clientPrivateKey);
      ClientRequest request = ClientRequest.newBuilder()
          .setTransaction(TransactionRequest.newBuilder().setRequestKey(ClientRequestKey.newBuilder().setClientSenderId(clientSenderId).setRequestId(requestId))
              .setType(TransactionType.TRANSACTION_TYPE_TRANSFER).setTo(TEST_RECIPIENT_ADDRESS).setAmount(requestId + 1).setNonce(nonce).setGasLimit(21_000L).setGasPrice(1L)
              .setSignature(ByteString.copyFrom(signature)))
          .build();
      NodeCommand command = NodeCommand.newBuilder().setTransactionBatch(TransactionBatchNodeCommand.newBuilder().addClientRequests(request)).build();
      String nodeHash = CryptoUtil.sha256Hex(HotStuffCryptoPayloads.nodeHashPayload(HotStuffSupport.GENESIS_NODE.getNodeHash(), view, command));
      return Node.newBuilder().setParentNodeHash(HotStuffSupport.GENESIS_NODE.getNodeHash()).setNodeHash(nodeHash).setViewNumber(view).setCommand(command).build();
    } catch (Exception exception) {
      throw new IllegalStateException("Could not build signed byzantine test node", exception);
    }
  }

  private static Node attackedNode(Message message) {
    return switch (message.getBodyCase()) {
      case PROPOSAL -> message.getProposal().getProposedNode();
      case PHASE_CERTIFICATE -> message.getPhaseCertificate().getJustifyQc().getCertifiedNode();
      default -> null;
    };
  }

  private static boolean isVoteTrigger(Message message) {
    return message.getMessageType() == ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE || message.getMessageType() == ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT
        || message.getMessageType() == ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_COMMIT;
  }

  private static String invalidSha256Hex(String originalHash) {
    char replacement = '0';
    if (originalHash.charAt(0) == '0') {
      replacement = '1';
    }
    return replacement + originalHash.substring(1);
  }

  private void sendInitialNewView(AuthenticatedLink nodeTransport) throws Exception {
    ConfigParser.ReplicaSection leader = configParser.replicas().getFirst();
    InetSocketAddress leaderAddress = new InetSocketAddress(leader.host(), leader.consensusPort());
    QuorumCertificate genesisQc = validGenesisQc();
    if (attackMode == ByzantineAttackMode.INVALID_NEW_VIEW) {
      genesisQc = invalidInitialNewViewQc();
    }

    Message newView = Message.newBuilder().setViewNumber(0).setReplicaSenderId(Math.toIntExact(replicaConfig.senderId()))
        .setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_NEW_VIEW).setPhaseCertificate(PhaseCertificateMessage.newBuilder().setJustifyQc(genesisQc)).build();
    nodeTransport.send(ThreadLocalRandom.current().nextLong(), ProtoValidationUtil.requireValid(newView, "ReplicaMessage").toByteArray(), leaderAddress);
    if (attackMode == ByzantineAttackMode.INVALID_NEW_VIEW) {
      observeAttack();
    }
  }

  private QuorumCertificate validGenesisQc() {
    return QuorumCertificate.newBuilder().setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_DECIDE).setViewNumber(-1).setCertifiedNode(HotStuffSupport.GENESIS_NODE)
        .build();
  }

  private QuorumCertificate invalidInitialNewViewQc() {
    return QuorumCertificate.newBuilder().setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_DECIDE).setViewNumber(0).setCertifiedNode(HotStuffSupport.GENESIS_NODE)
        .build();
  }

  private boolean isInitialLeader() {
    return configParser.replicas().getFirst().senderId() == replicaConfig.senderId();
  }

  private void observeAttack() {
    if (attackObserved.compareAndSet(false, true)) {
      logger.info("{} {}", ATTACK_MARKER, attackMode);
    }
  }

  private static Map<String, Long> buildReplicaSenderIdByConsensusEndpoint(ConfigParser configParser) {
    Map<String, Long> senderIdByEndpoint = new HashMap<>();
    for (ConfigParser.ReplicaSection replica : configParser.replicas()) {
      senderIdByEndpoint.put(endpointKey(replica.host(), replica.consensusPort()), replica.senderId());
    }
    return Map.copyOf(senderIdByEndpoint);
  }

  private static Map<Integer, InetSocketAddress> buildConsensusEndpointsBySenderId(ConfigParser configParser) {
    Map<Integer, InetSocketAddress> endpointsBySenderId = new HashMap<>();
    for (ConfigParser.ReplicaSection replica : configParser.replicas()) {
      endpointsBySenderId.put(Math.toIntExact(replica.senderId()), new InetSocketAddress(replica.host(), replica.consensusPort()));
    }
    return Map.copyOf(endpointsBySenderId);
  }

  private static String endpointKey(InetSocketAddress endpoint) {
    return endpointKey(endpoint.getAddress().getHostAddress(), endpoint.getPort());
  }

  private static String endpointKey(String host, int port) {
    return host + ":" + port;
  }
}
