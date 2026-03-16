package pt.ulisboa.depchain.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import pt.ulisboa.depchain.proto.AppendNodeCommand;
import pt.ulisboa.depchain.proto.AppendRequest;
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
import pt.ulisboa.depchain.proto.VoteMessage;
import pt.ulisboa.depchain.server.consensus.ConsensusCryptoPayloadUtil;
import pt.ulisboa.depchain.server.consensus.ConsensusTransportUtil;
import pt.ulisboa.depchain.server.consensus.ConsensusUtil;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.config.ConfigParser.ReplicaSection;
import pt.ulisboa.depchain.shared.keys.PrivateKeyLoader;
import pt.ulisboa.depchain.shared.keys.PublicKeyLoader;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.ClientRequestSignaturePayloadUtil;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;
import pt.ulisboa.depchain.shared.utils.ProtoValidationUtil;

abstract class IntegrationTestSupport {
  protected static final String LEADER_REPLICA_ID = "server1";
  protected static final String FOLLOWER_REPLICA_ID = "server2";
  protected static final String BYZANTINE_REPLICA_ID = "server3";
  protected static final String SECOND_BYZANTINE_REPLICA_ID = "server4";
  protected static final List<String> REPLICA_IDS = List.of("server1", "server2", "server3", "server4");
  protected static final List<String> HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS = List.of("server1", "server2", "server4");
  protected static final List<String> HONEST_REPLICA_IDS = List.of("server1", "server2");
  protected static final List<String> HONEST_WITH_BYZANTINE_LEADER_REPLICA_IDS = List.of("server2", "server3", "server4");

  protected enum ReplicaAttackMode {
    DROP_ALL_MESSAGES, INVALID_NEW_VIEW, SPOOFED_REPLICA_SENDER_ID, INVALID_PREPARE_VOTE, INVALID_PRE_COMMIT_VOTE, INVALID_COMMIT_VOTE, STALE_PREPARE_VOTE, STALE_PRE_COMMIT_VOTE, STALE_COMMIT_VOTE, INVALID_PREPARE_PROPOSAL_QC, INVALID_PRE_COMMIT_QC, INVALID_COMMIT_QC, INVALID_DECIDE_QC
  }

  protected record ProcessResult(int exitCode, String output) {
  }

  protected static Path integrationConfigPath() {
    Path configPath = Path.of(System.getProperty("user.dir"), "config", "config.yaml").toAbsolutePath();
    assertTrue(Files.exists(configPath), "Missing config file: " + configPath);
    return configPath;
  }

  protected static void populateConfig(Path configPath) throws IOException, InterruptedException {
    ProcessResult populateResult = runPopulate(configPath);
    assertEquals(0, populateResult.exitCode(), populateResult.output());
  }

  protected static InboundPacket sendForgedClientRequest(Path configPath, String replicaId, String command) throws Exception {
    ConfigParser config = ConfigParser.load(configPath);
    long clientSenderId = config.client().senderId();
    PrivateKey clientPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config);
    ClientRequest forgedRequest = forgedRequest(clientSenderId, command, clientPrivateKey);
    return sendPayloadToClientPort(configPath, replicaId, clientSenderId, clientPrivateKey, PublicKeyLoader.loadStaticPublicKeys(config), ProtoValidationUtil
        .requireValid(forgedRequest, "ClientRequest").toByteArray(), Duration.ofSeconds(3));
  }

  protected static InboundPacket broadcastClientRequestPayload(Path configPath, byte[] payload, Duration timeout) throws Exception {
    ConfigParser config = ConfigParser.load(configPath);
    long clientSenderId = config.client().senderId();
    PrivateKey clientPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config);
    Map<Long, PublicKey> staticPublicKeys = PublicKeyLoader.loadStaticPublicKeys(config);
    return broadcastAuthenticatedPayload(config, clientSenderId, clientPrivateKey, staticPublicKeys, payload, timeout);
  }

  protected static InboundPacket sendPayloadToClientPort(Path configPath, String replicaId, long senderId, PrivateKey privateKey, Map<Long, PublicKey> staticPublicKeys, byte[] payload, Duration timeout)
      throws Exception {
    ConfigParser config = ConfigParser.load(configPath);
    ReplicaSection targetReplica = config.requireReplicaById(replicaId);
    InetSocketAddress targetAddress = new InetSocketAddress(targetReplica.host(), targetReplica.clientPort());
    return sendAuthenticatedPayload(targetAddress, senderId, privateKey, staticPublicKeys, payload, timeout);
  }

  protected static InboundPacket sendPayloadToConsensusPort(Path configPath, String replicaId, long senderId, PrivateKey privateKey, Map<Long, PublicKey> staticPublicKeys, byte[] payload, Duration timeout)
      throws Exception {
    ConfigParser config = ConfigParser.load(configPath);
    ReplicaSection targetReplica = config.requireReplicaById(replicaId);
    InetSocketAddress targetAddress = new InetSocketAddress(targetReplica.host(), targetReplica.consensusPort());
    return sendAuthenticatedPayload(targetAddress, senderId, privateKey, staticPublicKeys, payload, timeout);
  }

  protected static ClientRequest signedRequest(Path configPath, String command) throws Exception {
    ConfigParser config = ConfigParser.load(configPath);
    long clientSenderId = config.client().senderId();
    PrivateKey clientPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config);
    long requestId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    return signedAppendRequest(clientSenderId, requestId, command, clientPrivateKey);
  }

  protected static void assertResponseNotNull(InboundPacket response, String message, List<StartedServer> servers) {
    if (response != null) {
      return;
    }

    StringBuilder failure = new StringBuilder(message).append(System.lineSeparator());
    for (StartedServer server : servers) {
      failure.append(server.describeState()).append(System.lineSeparator());
    }
    fail(failure.toString());
  }

  protected static ClientResponse decodeClientResponse(InboundPacket response) {
    try {
      return ProtoValidationUtil.requireValid(ClientResponse.parseFrom(response.packet().getPayload()), "ClientResponse");
    } catch (InvalidProtocolBufferException exception) {
      throw new IllegalArgumentException("Invalid protobuf client response payload", exception);
    }
  }

  protected static List<StartedServer> startServers(List<String> replicaIds, Path configPath) throws IOException {
    List<StartedServer> servers = new ArrayList<>();
    for (String replicaId : replicaIds) {
      servers.add(startServer(replicaId, configPath));
    }
    return servers;
  }

  protected static void waitForServersStartup(List<StartedServer> servers, Duration timeout) throws InterruptedException {
    long deadlineMs = System.currentTimeMillis() + timeout.toMillis();
    for (StartedServer server : servers) {
      long remainingMs = Math.max(1L, deadlineMs - System.currentTimeMillis());
      assertTrue(server.awaitReady(Duration.ofMillis(remainingMs)), "Server did not become ready: " + server.describeState());
    }
  }

  protected static void assertRequestSucceeds(Path configPath, String command, Duration timeout, List<StartedServer> servers, String message) throws Exception {
    ClientRequest request = signedRequest(configPath, command);
    byte[] payload = ProtoValidationUtil.requireValid(request, "ClientRequest").toByteArray();
    InboundPacket response = broadcastClientRequestPayload(configPath, payload, timeout);
    assertResponseNotNull(response, message, servers);
    assertEquals("Success: " + command, decodeClientResponse(response).getAppend().getMessage(), message);
  }

  protected static void assertReplayIsIgnored(Path configPath, String command, List<StartedServer> servers, String message) throws Exception {
    ClientRequest request = signedRequest(configPath, command);
    byte[] payload = ProtoValidationUtil.requireValid(request, "ClientRequest").toByteArray();
    InboundPacket firstResponse = broadcastClientRequestPayload(configPath, payload, Duration.ofSeconds(10));
    assertResponseNotNull(firstResponse, message + " (initial request)", servers);
    assertEquals("Success: " + command, decodeClientResponse(firstResponse).getAppend().getMessage(), message + " (initial request)");

    for (int i = 0; i < 3; i++) {
      InboundPacket replayResponse = broadcastClientRequestPayload(configPath, payload, Duration.ofSeconds(3));
      assertNull(replayResponse, message + " (replay " + (i + 1) + ")");
    }
  }

  protected static ClientRequest signedAppendRequest(long clientSenderId, long requestId, String command, PrivateKey clientPrivateKey) throws Exception {
    byte[] signature = CryptoUtil.signEcdsa(ClientRequestSignaturePayloadUtil.signedAppendRequestPayload(clientSenderId, requestId, command), clientPrivateKey);
    return ProtoValidationUtil.requireValid(ClientRequest.newBuilder().setAppend(AppendRequest.newBuilder()
        .setRequestKey(ClientRequestKey.newBuilder().setClientSenderId(clientSenderId).setRequestId(requestId)).setValue(command).setSignature(ByteString.copyFrom(signature)))
        .build(), "ClientRequest");
  }

  protected static void stopProcesses(List<StartedServer> servers) {
    for (StartedServer server : servers) {
      if (server.process().isAlive()) {
        server.process().destroy();
      }
    }

    for (StartedServer server : servers) {
      Process process = server.process();
      if (!process.isAlive()) {
        continue;
      }

      try {
        if (!process.waitFor(2, TimeUnit.SECONDS)) {
          process.destroyForcibly();
        }
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        process.destroyForcibly();
      }
    }

    for (StartedServer server : servers) {
      server.awaitOutputDrain(Duration.ofSeconds(1));
    }
  }

  private static InboundPacket sendAuthenticatedPayload(InetSocketAddress targetAddress, long senderId, PrivateKey privateKey, Map<Long, PublicKey> staticPublicKeys, byte[] payload, Duration timeout)
      throws Exception {
    long connectionId = ThreadLocalRandom.current().nextLong();

    try (AuthenticatedLink transport = AuthenticatedLink.unbound(senderId, privateKey, staticPublicKeys)) {
      transport.send(connectionId, payload, targetAddress);

      try {
        long deadlineMs = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadlineMs) {
          InboundPacket inbound = transport.receive(Math.max(1L, deadlineMs - System.currentTimeMillis()));
          if (inbound != null && inbound.packet().getConnectionId() == connectionId) {
            return inbound;
          }
        }

        return null;
      } finally {
        try {
          transport.closeConnection(connectionId, targetAddress);
        } catch (RuntimeException ignored) {
        }
      }
    }
  }

  private static InboundPacket broadcastAuthenticatedPayload(ConfigParser config, long senderId, PrivateKey privateKey, Map<Long, PublicKey> staticPublicKeys, byte[] payload, Duration timeout)
      throws Exception {
    Map<Long, InetSocketAddress> endpointsByConnectionId = new LinkedHashMap<>();
    int requiredReplyCount = config.system().f() + 1;

    try (AuthenticatedLink transport = AuthenticatedLink.unbound(senderId, privateKey, staticPublicKeys)) {
      for (ReplicaSection replica : config.replicas()) {
        long connectionId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
        InetSocketAddress endpoint = new InetSocketAddress(replica.host(), replica.clientPort());
        try {
          transport.send(connectionId, payload, endpoint);
          endpointsByConnectionId.put(connectionId, endpoint);
        } catch (RuntimeException exception) {
          // Some adversarial tests intentionally leave replica client ports unavailable.
        }
      }

      if (endpointsByConnectionId.isEmpty()) {
        return null;
      }

      long deadlineMs = System.currentTimeMillis() + timeout.toMillis();
      Set<Long> expectedReplicaSenderIds = PublicKeyLoader.loadReplicaPublicKeys(config).keySet();
      Map<Long, String> responseKeyByReplicaSender = new HashMap<>();
      Map<String, Integer> replyCounts = new HashMap<>();

      while (System.currentTimeMillis() < deadlineMs) {
        InboundPacket inbound = transport.receive(Math.max(1L, deadlineMs - System.currentTimeMillis()));
        if (inbound == null || !endpointsByConnectionId.containsKey(inbound.packet().getConnectionId())) {
          continue;
        }

        Long authenticatedSenderId = inbound.authenticatedSenderId();
        if (authenticatedSenderId == null || !expectedReplicaSenderIds.contains(authenticatedSenderId) || responseKeyByReplicaSender.containsKey(authenticatedSenderId)) {
          continue;
        }

        ClientResponse response = decodeClientResponse(inbound);
        String responseKey = Base64.getEncoder().encodeToString(ProtoValidationUtil.requireValid(response, "ClientResponse").toByteArray());
        responseKeyByReplicaSender.put(authenticatedSenderId, responseKey);

        int replyCount = replyCounts.merge(responseKey, 1, Integer::sum);
        if (replyCount >= requiredReplyCount) {
          return inbound;
        }
      }

      return null;
    } finally {
      // AuthenticatedLink.close() already tears down the sockets; per-connection shutdown is not
      // needed in tests because the transport is short-lived and local to this helper.
    }
  }

  private static ClientRequest forgedRequest(long clientSenderId, String command, PrivateKey clientPrivateKey) throws Exception {
    long requestId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    ClientRequest validRequest = signedAppendRequest(clientSenderId, requestId, command, clientPrivateKey);
    byte[] forgedSignature = validRequest.getAppend().getSignature().toByteArray();
    forgedSignature[0] ^= 0x01;
    return validRequest.toBuilder().setAppend(validRequest.getAppend().toBuilder().setSignature(ByteString.copyFrom(forgedSignature))).build();
  }

  private static StartedServer startServer(String replicaId, Path configPath) throws IOException {
    ProcessBuilder processBuilder = new ProcessBuilder(javaExecutable(), "-cp", System.getProperty("java.class.path"), "pt.ulisboa.depchain.server.Main", replicaId,
        configPath.toString());
    processBuilder.redirectErrorStream(true);
    return new StartedServer(replicaId, processBuilder.start());
  }

  private static ProcessResult runPopulate(Path configPath) throws IOException, InterruptedException {
    ProcessBuilder processBuilder = new ProcessBuilder(javaExecutable(), "-cp", System.getProperty("java.class.path"), "pt.ulisboa.depchain.populate.Populate",
        configPath.toString());
    processBuilder.redirectErrorStream(true);

    Process process = processBuilder.start();
    boolean finished = process.waitFor(Duration.ofSeconds(20).toMillis(), TimeUnit.MILLISECONDS);
    if (!finished) {
      process.destroyForcibly();
      return new ProcessResult(124, "Populate timeout");
    }

    return new ProcessResult(process.exitValue(), readAll(process.getInputStream()));
  }

  private static String javaExecutable() {
    String javaHome = System.getProperty("java.home");
    String suffix = "";
    if (System.getProperty("os.name").toLowerCase().contains("win")) {
      suffix = ".exe";
    }
    return Path.of(javaHome, "bin", "java" + suffix).toString();
  }

  private static String readAll(InputStream inputStream) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    inputStream.transferTo(output);
    return output.toString(StandardCharsets.UTF_8);
  }

  protected static final class ByzantineReplicaHandle implements AutoCloseable {
    private final AuthenticatedLink transport;
    private final ConfigParser config;
    private final int senderId;
    private final ReplicaAttackMode attackMode;
    private final Map<Integer, InetSocketAddress> consensusEndpointsBySenderId;
    private final AtomicInteger invalidVotesSent = new AtomicInteger();
    private final AtomicInteger attacksObserved = new AtomicInteger();
    private final Thread worker;
    private volatile boolean running = true;
    private volatile boolean sentInvalidVote;

    protected ByzantineReplicaHandle(Path configPath, String replicaId) throws Exception {
      this(configPath, replicaId, ReplicaAttackMode.INVALID_PREPARE_VOTE);
    }

    protected ByzantineReplicaHandle(Path configPath, String replicaId, ReplicaAttackMode attackMode) throws Exception {
      this.config = ConfigParser.load(configPath);
      ConfigParser.ReplicaSection replica = this.config.requireReplicaById(replicaId);
      PrivateKey privateKey = PrivateKeyLoader.loadReplicaPrivateKey(config, replica.senderId());
      Map<Long, PublicKey> staticPublicKeys = PublicKeyLoader.loadStaticPublicKeys(config);
      InetSocketAddress bindEndpoint = new InetSocketAddress(replica.host(), replica.consensusPort());

      this.transport = AuthenticatedLink.bind(bindEndpoint, replica.senderId(), privateKey, staticPublicKeys);
      this.senderId = (int) replica.senderId();
      this.attackMode = attackMode;
      this.consensusEndpointsBySenderId = buildConsensusEndpointsBySenderId(config);
      sendInitialNewView(config);
      this.worker = Thread.ofVirtual().name("byzantine-replica-" + replicaId).start(this::runLoop);
    }

    protected int invalidVotesSent() {
      return invalidVotesSent.get();
    }

    protected boolean attackObserved() {
      return attacksObserved.get() > 0;
    }

    private void runLoop() {
      while (running) {
        try {
          InboundPacket inbound = transport.receive(200);
          if (inbound == null) {
            continue;
          }

          Message message = ProtoValidationUtil.requireValid(Message.parseFrom(inbound.packet().getPayload()), "ReplicaMessage");
          switch (attackMode) {
            case DROP_ALL_MESSAGES -> attacksObserved.compareAndSet(0, 1);
            case INVALID_NEW_VIEW -> attacksObserved.compareAndSet(0, 1);
            case SPOOFED_REPLICA_SENDER_ID -> {
              if (!sentInvalidVote && isVoteTrigger(message)) {
                sendSpoofedSenderVote(inbound, message);
              }
            }
            case INVALID_PREPARE_VOTE -> maybeSendInvalidVote(inbound, message, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, false);
            case INVALID_PRE_COMMIT_VOTE -> maybeSendInvalidVote(inbound, message, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT, false);
            case INVALID_COMMIT_VOTE -> maybeSendInvalidVote(inbound, message, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_COMMIT, false);
            case STALE_PREPARE_VOTE -> maybeSendInvalidVote(inbound, message, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, true);
            case STALE_PRE_COMMIT_VOTE -> maybeSendInvalidVote(inbound, message, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT, true);
            case STALE_COMMIT_VOTE -> maybeSendInvalidVote(inbound, message, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_COMMIT, true);
            case INVALID_PREPARE_PROPOSAL_QC -> maybeBroadcastInvalidLeaderMessage(message, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE);
            case INVALID_PRE_COMMIT_QC -> maybeBroadcastInvalidLeaderMessage(message, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT);
            case INVALID_COMMIT_QC -> maybeBroadcastInvalidLeaderMessage(message, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_COMMIT);
            case INVALID_DECIDE_QC -> maybeBroadcastInvalidLeaderMessage(message, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_DECIDE);
          }
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          return;
        } catch (Exception ignored) {
          if (!running) {
            return;
          }
        }
      }
    }

    private void maybeSendInvalidVote(InboundPacket inbound, Message message, ConsensusMessageType expectedType, boolean staleView) throws Exception {
      if (!sentInvalidVote && message.getMessageType() == expectedType) {
        int voteView = message.getViewNumber();
        if (staleView) {
          voteView = Math.max(0, message.getViewNumber() - 1);
        }
        sendInvalidVote(inbound, message, expectedType, voteView, staleView);
      }
    }

    private void sendInvalidVote(InboundPacket inbound, Message message, ConsensusMessageType voteType, int voteView, boolean keepNodeHash) throws Exception {
      Node originalNode = switch (message.getBodyCase()) {
        case PROPOSAL -> message.getProposal().getProposedNode();
        case PHASE_CERTIFICATE -> message.getPhaseCertificate().getJustifyQc().getCertifiedNode();
        default -> null;
      };
      if (originalNode == null) {
        return;
      }

      Node votedNode = originalNode;
      if (!keepNodeHash) {
        votedNode = originalNode.toBuilder().setNodeHash(invalidSha256Hex(originalNode.getNodeHash())).build();
      }
      Message invalidVote = Message.newBuilder().setViewNumber(voteView).setReplicaSenderId(senderId).setMessageType(voteType).setVote(VoteMessage.newBuilder()
          .setVotedNode(votedNode).setThresholdSignatureShare(ByteString.copyFrom(new byte[32])).setAggregatedCommitment(ByteString.copyFrom(new byte[32]))).build();
      transport.send(inbound.packet().getConnectionId(), ProtoValidationUtil.requireValid(invalidVote, "ReplicaMessage").toByteArray(), inbound.sender());
      invalidVotesSent.incrementAndGet();
      attacksObserved.incrementAndGet();
      sentInvalidVote = true;
    }

    private void maybeBroadcastInvalidLeaderMessage(Message message, ConsensusMessageType maliciousType) throws Exception {
      if (sentInvalidVote || message.getMessageType() != ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_NEW_VIEW) {
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
      for (Map.Entry<Integer, InetSocketAddress> entry : consensusEndpointsBySenderId.entrySet()) {
        if (entry.getKey() == senderId) {
          continue;
        }
        transport.send(ConsensusTransportUtil
            .connectionIdForView(maliciousMessage.getViewNumber(), senderId, entry.getKey(), ConsensusTransportUtil.REPLICA_MESSAGE_LANE), payload, entry.getValue());
      }
      invalidVotesSent.incrementAndGet();
      attacksObserved.incrementAndGet();
      sentInvalidVote = true;
    }

    private void sendSpoofedSenderVote(InboundPacket inbound, Message message) throws Exception {
      Node originalNode = switch (message.getBodyCase()) {
        case PROPOSAL -> message.getProposal().getProposedNode();
        case PHASE_CERTIFICATE -> message.getPhaseCertificate().getJustifyQc().getCertifiedNode();
        default -> null;
      };
      if (originalNode == null) {
        return;
      }

      Message spoofedVote = Message.newBuilder().setViewNumber(message.getViewNumber()).setReplicaSenderId(1).setMessageType(message.getMessageType()).setVote(VoteMessage
          .newBuilder().setVotedNode(originalNode).setThresholdSignatureShare(ByteString.copyFrom(new byte[32])).setAggregatedCommitment(ByteString.copyFrom(new byte[32])))
          .build();
      transport.send(inbound.packet().getConnectionId(), ProtoValidationUtil.requireValid(spoofedVote, "ReplicaMessage").toByteArray(), inbound.sender());
      invalidVotesSent.incrementAndGet();
      attacksObserved.incrementAndGet();
      sentInvalidVote = true;
    }

    private boolean isVoteTrigger(Message message) {
      return message.getMessageType() == ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE || message.getMessageType() == ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT
          || message.getMessageType() == ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_COMMIT;
    }

    private Message invalidPrepareProposal() {
      Node proposedNode = validLeafNode(0, 0L, "invalid-proposal");
      QuorumCertificate invalidJustifyQc = invalidQc(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 0, ConsensusUtil.GENESIS_NODE);
      return Message.newBuilder().setViewNumber(0).setReplicaSenderId(senderId).setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE)
          .setProposal(ProposalMessage.newBuilder().setProposedNode(proposedNode).setJustifyQc(invalidJustifyQc)).build();
    }

    private Message invalidPhaseCertificate(ConsensusMessageType maliciousType) {
      ConsensusMessageType qcType = switch (maliciousType) {
        case CONSENSUS_MESSAGE_TYPE_PRE_COMMIT -> ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE;
        case CONSENSUS_MESSAGE_TYPE_COMMIT -> ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT;
        case CONSENSUS_MESSAGE_TYPE_DECIDE -> ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_COMMIT;
        default -> throw new IllegalArgumentException("Unsupported malicious phase type " + maliciousType);
      };
      QuorumCertificate invalidJustifyQc = invalidQc(qcType, 0, validLeafNode(0, 1L, "invalid-qc-node"));
      return Message.newBuilder().setViewNumber(0).setReplicaSenderId(senderId).setMessageType(maliciousType)
          .setPhaseCertificate(PhaseCertificateMessage.newBuilder().setJustifyQc(invalidJustifyQc)).build();
    }

    private QuorumCertificate invalidQc(ConsensusMessageType qcType, int qcView, Node certifiedNode) {
      return QuorumCertificate.newBuilder().setMessageType(qcType).setViewNumber(qcView).setCertifiedNode(certifiedNode).setQuorumSignature(ByteString.copyFrom(new byte[64]))
          .build();
    }

    private Node validLeafNode(int view, long requestId, String value) {
      try {
        ClientRequest request = signedAppendRequest(config.client().senderId(), requestId, value, PrivateKeyLoader.loadClientPrivateKey(config));
        NodeCommand command = NodeCommand.newBuilder().setAppend(AppendNodeCommand.newBuilder().setClientRequest(request)).build();
        String nodeHash = CryptoUtil.sha256Hex(ConsensusCryptoPayloadUtil.nodeHashPayload(ConsensusUtil.GENESIS_NODE.getNodeHash(), view, command));
        return Node.newBuilder().setParentNodeHash(ConsensusUtil.GENESIS_NODE.getNodeHash()).setNodeHash(nodeHash).setViewNumber(view).setCommand(command).build();
      } catch (Exception exception) {
        throw new IllegalStateException("Could not build signed malicious test node", exception);
      }
    }

    private String invalidSha256Hex(String originalHash) {
      char replacement = '0';
      if (originalHash.charAt(0) == '0') {
        replacement = '1';
      }
      return replacement + originalHash.substring(1);
    }

    private void sendInitialNewView(ConfigParser config) throws Exception {
      ConfigParser.ReplicaSection leader = config.replicas().getFirst();
      InetSocketAddress leaderAddress = new InetSocketAddress(leader.host(), leader.consensusPort());
      if (attackMode == ReplicaAttackMode.DROP_ALL_MESSAGES) {
        attacksObserved.incrementAndGet();
        return;
      }

      QuorumCertificate genesisQc;
      if (attackMode == ReplicaAttackMode.INVALID_NEW_VIEW) {
        genesisQc = QuorumCertificate.newBuilder().setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_DECIDE).setViewNumber(0).setCertifiedNode(ConsensusUtil.GENESIS_NODE)
            .build();
      } else {
        genesisQc = QuorumCertificate.newBuilder().setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_DECIDE).setViewNumber(-1).setCertifiedNode(ConsensusUtil.GENESIS_NODE)
            .build();
      }
      Message newView = Message.newBuilder().setViewNumber(0).setReplicaSenderId(senderId).setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_NEW_VIEW)
          .setPhaseCertificate(PhaseCertificateMessage.newBuilder().setJustifyQc(genesisQc)).build();
      long connectionId = ThreadLocalRandom.current().nextLong();
      transport.send(connectionId, ProtoValidationUtil.requireValid(newView, "ReplicaMessage").toByteArray(), leaderAddress);
      attacksObserved.incrementAndGet();
    }

    @Override
    public void close() throws Exception {
      running = false;
      transport.close();
      worker.join(TimeUnit.SECONDS.toMillis(1));
    }
  }

  private static Map<Integer, InetSocketAddress> buildConsensusEndpointsBySenderId(ConfigParser config) {
    Map<Integer, InetSocketAddress> endpointsBySenderId = new LinkedHashMap<>();
    for (ReplicaSection replica : config.replicas()) {
      endpointsBySenderId.put(Math.toIntExact(replica.senderId()), new InetSocketAddress(replica.host(), replica.consensusPort()));
    }
    return Map.copyOf(endpointsBySenderId);
  }

  protected static final class StartedServer {
    private static final String CLIENT_LISTENER_MARKER = " client listener: ";
    private static final String NODE_LISTENER_MARKER = " node listener: ";

    private final String replicaId;
    private final Process process;
    private final StringBuilder output = new StringBuilder();
    private final CountDownLatch ready = new CountDownLatch(2);
    private final AtomicBoolean clientReady = new AtomicBoolean();
    private final AtomicBoolean nodeReady = new AtomicBoolean();
    private final Thread outputReader;

    private StartedServer(String replicaId, Process process) {
      this.replicaId = replicaId;
      this.process = process;
      this.outputReader = Thread.ofVirtual().name("server-output-" + replicaId).start(this::readOutputLoop);
    }

    private Process process() {
      return process;
    }

    private boolean awaitReady(Duration timeout) throws InterruptedException {
      if (ready.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
        return process.isAlive();
      }
      return false;
    }

    private void awaitOutputDrain(Duration timeout) {
      try {
        outputReader.join(timeout.toMillis());
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
    }

    private String describeState() {
      String state;
      if (process.isAlive()) {
        state = "alive";
      } else {
        state = "exited(" + process.exitValue() + ")";
      }
      return "Server " + replicaId + " [" + state + ", clientReady=" + clientReady.get() + ", nodeReady=" + nodeReady.get() + "]\n" + outputSnapshot();
    }

    private void readOutputLoop() {
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          synchronized (output) {
            output.append(line).append(System.lineSeparator());
          }

          if (line.contains("Replica " + replicaId + CLIENT_LISTENER_MARKER) && clientReady.compareAndSet(false, true)) {
            ready.countDown();
          }
          if (line.contains("Replica " + replicaId + NODE_LISTENER_MARKER) && nodeReady.compareAndSet(false, true)) {
            ready.countDown();
          }
        }
      } catch (IOException ignored) {
      }
    }

    private String outputSnapshot() {
      synchronized (output) {
        return output.toString();
      }
    }
  }
}
