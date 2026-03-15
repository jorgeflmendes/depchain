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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import pt.ulisboa.depchain.proto.AppendRequest;
import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.ClientResponse;
import pt.ulisboa.depchain.proto.ConsensusMessageType;
import pt.ulisboa.depchain.proto.Message;
import pt.ulisboa.depchain.proto.Node;
import pt.ulisboa.depchain.proto.PhaseCertificateMessage;
import pt.ulisboa.depchain.proto.QuorumCertificate;
import pt.ulisboa.depchain.proto.VoteMessage;
import pt.ulisboa.depchain.server.consensus.ConsensusUtil;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.keys.PrivateKeyLoader;
import pt.ulisboa.depchain.shared.keys.PublicKeyLoader;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.ClientRequestPayloadUtil;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;
import pt.ulisboa.depchain.shared.utils.ProtoValidationUtil;

@Tag("integration")
class SecTest {
  private static final List<String> REPLICA_IDS = List.of("server1", "server2", "server3", "server4");
  private static final List<String> HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS = List.of("server1", "server2", "server4");
  private static final List<String> HONEST_REPLICA_IDS = List.of("server1", "server2");

  private record ProcessResult(int exitCode, String output) {
  }

  @Test
  @Timeout(60)
  void normalExecutionTest() throws Exception {
    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    List<StartedServer> servers = startServers(REPLICA_IDS, configPath);
    try {
      waitForServersStartup(servers, Duration.ofSeconds(10));

      // Four valid requests to the initial leader should get success replies.
      for (int i = 1; i <= 4; i++) {
        String value = "simple-test-" + i;
        ClientRequest request = signedRequest(configPath, value);
        byte[] payload = ProtoValidationUtil.requireValid(request, "ClientRequest").toByteArray();
        InboundPacket response = sendClientRequestPayload(configPath, "server1", payload, Duration.ofSeconds(45));
        assertResponseNotNull(response, "Client request should receive a response", servers);
        assertEquals("Success: " + value, decodeClientResponse(response).getAppend().getMessage());
      }
    } finally {
      stopProcesses(servers);
    }
  }

  @Test
  @Timeout(90)
  void forwardedClientRequestTest() throws Exception {
    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    List<StartedServer> servers = startServers(REPLICA_IDS, configPath);
    try {
      waitForServersStartup(servers, Duration.ofSeconds(15));

      // A request sent to a non-leader should be forwarded to the leader and still succeed.
      ClientRequest request = signedRequest(configPath, "forwarded-test");
      byte[] payload = ProtoValidationUtil.requireValid(request, "ClientRequest").toByteArray();
      InboundPacket response = sendClientRequestPayload(configPath, "server2", payload, Duration.ofSeconds(45));
      assertResponseNotNull(response, "Forwarded client request should receive a response", servers);
      assertEquals("Success: forwarded-test", decodeClientResponse(response).getAppend().getMessage());
    } finally {
      stopProcesses(servers);
    }
  }

  @Test
  @Timeout(60)
  void replayedClientRequestTest() throws Exception {
    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    List<StartedServer> servers = startServers(REPLICA_IDS, configPath);
    try {
      waitForServersStartup(servers, Duration.ofSeconds(15));

      // Build one real signed request and execute it successfully once.
      ClientRequest request = signedRequest(configPath, "replayed-test");
      byte[] payload = ProtoValidationUtil.requireValid(request, "ClientRequest").toByteArray();
      InboundPacket firstResponse = sendClientRequestPayload(configPath, "server1", payload, Duration.ofSeconds(10));
      assertResponseNotNull(firstResponse, "Initial client request should receive a response", servers);
      assertEquals("Success: replayed-test", decodeClientResponse(firstResponse).getAppend().getMessage());

      // Replaying the exact same signed request should be ignored by deduplication.
      for (int i = 0; i < 10; i++) {
        InboundPacket replayResponse = sendClientRequestPayload(configPath, "server1", payload, Duration.ofSeconds(3));
        assertNull(replayResponse, "Replayed client request should not receive a response");
      }
    } finally {
      stopProcesses(servers);
    }
  }

  @Test
  @Timeout(60)
  void forgedClientSignatureTest() throws Exception {
    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    List<StartedServer> servers = startServers(REPLICA_IDS, configPath);
    try {
      waitForServersStartup(servers, Duration.ofSeconds(15));

      // A forged client signature should be ignored.
      InboundPacket response = sendForgedClientRequest(configPath, "server1", "forged-test");
      assertNull(response, "Forged client request should not receive a response");
    } finally {
      stopProcesses(servers);
    }
  }

  @Test
  @Timeout(60)
  void oneByzantineReplicaInvalidVoteTest() throws Exception {
    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    List<StartedServer> servers = startServers(HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS, configPath);
    try {
      try (ByzantineReplicaHandle byzantineReplica3 = new ByzantineReplicaHandle(configPath, "server3")) {
        waitForServersStartup(servers, Duration.ofSeconds(15));

        // With only one Byzantine invalid voter, the honest replicas should still complete the request.
        ClientRequest request = signedRequest(configPath, "one-byzantine-test");
        byte[] payload = ProtoValidationUtil.requireValid(request, "ClientRequest").toByteArray();
        InboundPacket response = sendClientRequestPayload(configPath, "server1", payload, Duration.ofSeconds(20));
        assertResponseNotNull(response, "Client request should still receive a response with one Byzantine invalid vote", servers);
        assertEquals("Success: one-byzantine-test", decodeClientResponse(response).getAppend().getMessage());
        assertTrue(byzantineReplica3.invalidVotesSent() > 0, "Byzantine replica server3 did not send any invalid vote");
      }
    } finally {
      stopProcesses(servers);
    }
  }

  @Test
  @Timeout(60)
  void twoByzantineReplicasInvalidVoteTest() throws Exception {
    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    List<StartedServer> servers = startServers(HONEST_REPLICA_IDS, configPath);
    try {
      try (ByzantineReplicaHandle byzantineReplica3 = new ByzantineReplicaHandle(configPath, "server3");
          ByzantineReplicaHandle byzantineReplica4 = new ByzantineReplicaHandle(configPath, "server4")) {
        waitForServersStartup(servers, Duration.ofSeconds(15));

        // With two Byzantine invalid voters, the leader should be unable to gather enough honest votes.
        ClientRequest request = signedRequest(configPath, "byzantine-test");
        byte[] payload = ProtoValidationUtil.requireValid(request, "ClientRequest").toByteArray();
        InboundPacket response = sendClientRequestPayload(configPath, "server1", payload, Duration.ofSeconds(10));
        assertNull(response, "Client request should time out when two Byzantine replicas prevent quorum");
        assertTrue(byzantineReplica3.invalidVotesSent() > 0, "Byzantine replica server3 did not send any invalid vote");
        assertTrue(byzantineReplica4.invalidVotesSent() > 0, "Byzantine replica server4 did not send any invalid vote");
      }
    } finally {
      stopProcesses(servers);
    }
  }

  private static InboundPacket sendForgedClientRequest(Path configPath, String targetReplicaId, String command) throws Exception {
    ConfigParser config = ConfigParser.load(configPath);
    long clientSenderId = config.client().senderId();
    PrivateKey clientPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config);
    ClientRequest forgedRequest = forgedRequest(clientSenderId, command, clientPrivateKey);
    byte[] payload = ProtoValidationUtil.requireValid(forgedRequest, "ClientRequest").toByteArray();
    return sendClientRequestPayload(configPath, targetReplicaId, payload, Duration.ofSeconds(3));
  }

  private static InboundPacket sendClientRequestPayload(Path configPath, String targetReplicaId, byte[] payload, Duration timeout) throws Exception {
    // Load the real client identity and the target replica endpoint.
    ConfigParser config = ConfigParser.load(configPath);
    ConfigParser.ReplicaSection targetReplica = config.requireReplicaById(targetReplicaId);
    long clientSenderId = config.client().senderId();
    PrivateKey clientPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config);
    Map<Long, PublicKey> staticPublicKeys = PublicKeyLoader.loadStaticPublicKeys(config);
    InetSocketAddress targetAddress = new InetSocketAddress(targetReplica.host(), targetReplica.clientPort());
    long connectionId = ThreadLocalRandom.current().nextLong();

    try (AuthenticatedLink transport = AuthenticatedLink.unbound(clientSenderId, clientPrivateKey, staticPublicKeys)) {
      // Send exactly the provided client request bytes and wait only for this connection's reply.
      transport.send(connectionId, payload, targetAddress);

      try {
        long deadlineMs = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadlineMs) {
          InboundPacket inbound = transport.receive(Math.max(1L, deadlineMs - System.currentTimeMillis()));
          if (inbound == null) {
            continue;
          }

          if (inbound.packet().getConnectionId() == connectionId) {
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

  private static ClientRequest signedRequest(Path configPath, String command) throws Exception {
    // Build a normal client request with a fresh requestId and a valid client signature.
    ConfigParser config = ConfigParser.load(configPath);
    long clientSenderId = config.client().senderId();
    PrivateKey clientPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config);
    long requestId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    return signedAppendRequest(clientSenderId, requestId, command, clientPrivateKey);
  }

  private static ClientRequest forgedRequest(long clientSenderId, String command, PrivateKey clientPrivateKey) throws Exception {
    // Start from a correctly signed client request.
    long requestId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    ClientRequest validRequest = signedAppendRequest(clientSenderId, requestId, command, clientPrivateKey);

    // Flip one byte so the request keeps its shape but fails signature verification.
    byte[] forgedSignature = validRequest.getAppend().getSignature().toByteArray();
    forgedSignature[0] ^= 0x01;
    return validRequest.toBuilder().setAppend(validRequest.getAppend().toBuilder().setSignature(ByteString.copyFrom(forgedSignature))).build();
  }

  private static Path integrationConfigPath() {
    Path configPath = Path.of(System.getProperty("user.dir"), "config", "config.yaml").toAbsolutePath();
    assertTrue(Files.exists(configPath), "Missing config file: " + configPath);
    return configPath;
  }

  private static void populateConfig(Path configPath) throws IOException, InterruptedException {
    ProcessResult populateResult = runPopulate(configPath);
    assertEquals(0, populateResult.exitCode(), populateResult.output());
  }

  private static void assertResponseNotNull(InboundPacket response, String message, List<StartedServer> servers) {
    if (response != null) {
      return;
    }

    StringBuilder failure = new StringBuilder(message).append(System.lineSeparator());
    for (StartedServer server : servers) {
      failure.append(server.describeState()).append(System.lineSeparator());
    }
    fail(failure.toString());
  }

  private static ClientResponse decodeClientResponse(InboundPacket response) {
    try {
      return ProtoValidationUtil.requireValid(ClientResponse.parseFrom(response.packet().getPayload()), "ClientResponse");
    } catch (InvalidProtocolBufferException exception) {
      throw new IllegalArgumentException("Invalid protobuf client response payload", exception);
    }
  }

  private static List<StartedServer> startServers(List<String> replicaIds, Path configPath) throws IOException {
    List<StartedServer> servers = new ArrayList<>();
    for (String replicaId : replicaIds) {
      servers.add(startServer(replicaId, configPath));
    }
    return servers;
  }

  private static ClientRequest signedAppendRequest(long clientSenderId, long requestId, String command, PrivateKey clientPrivateKey) throws Exception {
    byte[] signature = CryptoUtil.signEcdsa(ClientRequestPayloadUtil.signedAppendRequestPayload(clientSenderId, requestId, command), clientPrivateKey);
    return ProtoValidationUtil.requireValid(ClientRequest.newBuilder().setAppend(AppendRequest.newBuilder()
        .setRequestKey(ClientRequestKey.newBuilder().setClientSenderId(clientSenderId).setRequestId(requestId)).setValue(command).setSignature(ByteString.copyFrom(signature)))
        .build(), "ClientRequest");
  }

  private static void waitForServersStartup(List<StartedServer> servers, Duration timeout) throws InterruptedException {
    long deadlineMs = System.currentTimeMillis() + timeout.toMillis();
    for (StartedServer server : servers) {
      long remainingMs = Math.max(1L, deadlineMs - System.currentTimeMillis());
      assertTrue(server.awaitReady(Duration.ofMillis(remainingMs)), "Server did not become ready: " + server.describeState());
    }
  }

  private static final class ByzantineReplicaHandle implements AutoCloseable {
    private final AuthenticatedLink transport;
    private final int senderId;
    private final AtomicInteger invalidVotesSent = new AtomicInteger();
    private final Thread worker;
    private volatile boolean running = true;
    private volatile boolean sentInvalidVote;

    private ByzantineReplicaHandle(Path configPath, String replicaId) throws Exception {
      ConfigParser config = ConfigParser.load(configPath);
      ConfigParser.ReplicaSection replica = config.requireReplicaById(replicaId);
      PrivateKey privateKey = PrivateKeyLoader.loadReplicaPrivateKey(config, replica.senderId());
      Map<Long, PublicKey> staticPublicKeys = PublicKeyLoader.loadStaticPublicKeys(config);
      InetSocketAddress bindEndpoint = new InetSocketAddress(replica.host(), replica.consensusPort());

      this.transport = AuthenticatedLink.bind(bindEndpoint, replica.senderId(), privateKey, staticPublicKeys);
      this.senderId = (int) replica.senderId();
      sendInitialNewView(config);
      this.worker = Thread.ofVirtual().name("byzantine-replica-" + replicaId).start(this::runLoop);
    }

    private int invalidVotesSent() {
      return invalidVotesSent.get();
    }

    private void runLoop() {
      while (running) {
        try {
          InboundPacket inbound = transport.receive(200);
          if (inbound == null) {
            continue;
          }

          Message message = ProtoValidationUtil.requireValid(Message.parseFrom(inbound.packet().getPayload()), "ReplicaMessage");
          if (!sentInvalidVote && message.getMessageType() == ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE) {
            sendInvalidPrepareVote(inbound, message);
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

    private void sendInvalidPrepareVote(InboundPacket inbound, Message message) throws Exception {
      Node originalNode = message.hasProposal() ? message.getProposal().getProposedNode() : null;
      if (originalNode == null) {
        return;
      }

      // Send an explicit vote-shaped message with a mismatched node hash so the leader rejects it.
      Node invalidNode = originalNode.toBuilder().setNodeHash(invalidSha256Hex(originalNode.getNodeHash())).build();
      Message invalidVote = Message.newBuilder().setViewNumber(message.getViewNumber()).setReplicaSenderId(senderId)
          .setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE)
          .setVote(VoteMessage.newBuilder().setVotedNode(invalidNode).setThresholdSignatureShare(ByteString.copyFrom(new byte[32]))).build();
      transport.send(inbound.packet().getConnectionId(), ProtoValidationUtil.requireValid(invalidVote, "ReplicaMessage").toByteArray(), inbound.sender());
      invalidVotesSent.incrementAndGet();
      sentInvalidVote = true;
    }

    private String invalidSha256Hex(String originalHash) {
      char replacement = originalHash.charAt(0) == '0' ? '1' : '0';
      return replacement + originalHash.substring(1);
    }

    private void sendInitialNewView(ConfigParser config) throws Exception {
      ConfigParser.ReplicaSection leader = config.replicas().getFirst();
      InetSocketAddress leaderAddress = new InetSocketAddress(leader.host(), leader.consensusPort());
      QuorumCertificate genesisQC = QuorumCertificate.newBuilder().setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_DECIDE).setViewNumber(-1)
          .setCertifiedNode(ConsensusUtil.GENESIS_NODE).build();
      Message newView = Message.newBuilder().setViewNumber(0).setReplicaSenderId(senderId).setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_NEW_VIEW)
          .setPhaseCertificate(PhaseCertificateMessage.newBuilder().setJustifyQc(genesisQC)).build();
      long connectionId = ThreadLocalRandom.current().nextLong();
      transport.send(connectionId, ProtoValidationUtil.requireValid(newView, "ReplicaMessage").toByteArray(), leaderAddress);
    }

    @Override
    public void close() throws Exception {
      running = false;
      transport.close();
      worker.join(TimeUnit.SECONDS.toMillis(1));
    }
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

  private static void stopProcesses(List<StartedServer> servers) {
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

  private static final class StartedServer {
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
