package pt.ulisboa.depchain.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import pt.ulisboa.depchain.server.consensus.Message;
import pt.ulisboa.depchain.server.consensus.Message.MessageType;
import pt.ulisboa.depchain.server.consensus.Node;
import pt.ulisboa.depchain.server.consensus.QuorumCertificate;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.keys.PrivateKeyLoader;
import pt.ulisboa.depchain.shared.keys.PublicKeyLoader;
import pt.ulisboa.depchain.shared.model.ClientRequest;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.SerializationUtil;

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

    List<Process> servers = startServers(REPLICA_IDS, configPath);
    try {
      waitForServersStartup(Duration.ofSeconds(2));

      // Four valid requests to the initial leader should get success replies.
      for (int i = 1; i <= 4; i++) {
        String value = "simple-test-" + i;
        ClientRequest request = signedRequest(configPath, value);
        byte[] payload = SerializationUtil.encodeClientRequestBytes(request);
        InboundPacket response = sendClientRequestPayload(configPath, "server1", payload, Duration.ofSeconds(20));
        assertTrue(response != null, "Client request should receive a response");
        assertEquals("Success: " + value, SerializationUtil.decodeString(response.packet().payload()));
      }
    } finally {
      stopProcesses(servers);
    }
  }

  @Test
  @Timeout(60)
  void forwardedClientRequestTest() throws Exception {
    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    List<Process> servers = startServers(REPLICA_IDS, configPath);
    try {
      waitForServersStartup(Duration.ofSeconds(5));

      // A request sent to a non-leader should be forwarded to the leader and still succeed.
      ClientRequest request = signedRequest(configPath, "forwarded-test");
      byte[] payload = SerializationUtil.encodeClientRequestBytes(request);
      InboundPacket response = sendClientRequestPayload(configPath, "server2", payload, Duration.ofSeconds(20));
      assertTrue(response != null, "Forwarded client request should receive a response");
      assertEquals("Success: forwarded-test", SerializationUtil.decodeString(response.packet().payload()));
    } finally {
      stopProcesses(servers);
    }
  }

  @Test
  @Timeout(60)
  void replayedClientRequestTest() throws Exception {
    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    List<Process> servers = startServers(REPLICA_IDS, configPath);
    try {
      waitForServersStartup(Duration.ofSeconds(5));

      // Build one real signed request and execute it successfully once.
      ClientRequest request = signedRequest(configPath, "replayed-test");
      byte[] payload = SerializationUtil.encodeClientRequestBytes(request);
      InboundPacket firstResponse = sendClientRequestPayload(configPath, "server1", payload, Duration.ofSeconds(10));
      assertTrue(firstResponse != null, "Initial client request should receive a response");
      assertEquals("Success: replayed-test", SerializationUtil.decodeString(firstResponse.packet().payload()));

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

    List<Process> servers = startServers(REPLICA_IDS, configPath);
    try {
      waitForServersStartup(Duration.ofSeconds(5));

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

    List<Process> servers = startServers(HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS, configPath);
    try {
      try (ByzantineReplicaHandle byzantineReplica3 = new ByzantineReplicaHandle(configPath, "server3")) {
        waitForServersStartup(Duration.ofSeconds(5));

        // With only one Byzantine invalid voter, the honest replicas should still complete the request.
        ClientRequest request = signedRequest(configPath, "one-byzantine-test");
        byte[] payload = SerializationUtil.encodeClientRequestBytes(request);
        InboundPacket response = sendClientRequestPayload(configPath, "server1", payload, Duration.ofSeconds(20));
        assertTrue(response != null, "Client request should still receive a response with one Byzantine invalid vote");
        assertEquals("Success: one-byzantine-test", SerializationUtil.decodeString(response.packet().payload()));
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

    List<Process> servers = startServers(HONEST_REPLICA_IDS, configPath);
    try {
      try (ByzantineReplicaHandle byzantineReplica3 = new ByzantineReplicaHandle(configPath, "server3");
          ByzantineReplicaHandle byzantineReplica4 = new ByzantineReplicaHandle(configPath, "server4")) {
        waitForServersStartup(Duration.ofSeconds(5));

        // With two Byzantine invalid voters, the leader should be unable to gather enough honest votes.
        ClientRequest request = signedRequest(configPath, "byzantine-test");
        byte[] payload = SerializationUtil.encodeClientRequestBytes(request);
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
    byte[] payload = SerializationUtil.encodeClientRequestBytes(forgedRequest);
    return sendClientRequestPayload(configPath, targetReplicaId, payload, Duration.ofSeconds(3));
  }

  private static InboundPacket sendClientRequestPayload(Path configPath, String targetReplicaId, byte[] payload, Duration timeout) throws Exception {
    // Load the real client identity and the target replica endpoint.
    ConfigParser config = ConfigParser.load(configPath);
    ConfigParser.ReplicaSection targetReplica = config.requireReplica(targetReplicaId);
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

          if (inbound.packet().connectionId() == connectionId) {
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
    return ClientRequest.signed(clientSenderId, requestId, command, clientPrivateKey);
  }

  private static ClientRequest forgedRequest(long clientSenderId, String command, PrivateKey clientPrivateKey) throws Exception {
    // Start from a correctly signed client request.
    long requestId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    ClientRequest validRequest = ClientRequest.signed(clientSenderId, requestId, command, clientPrivateKey);

    // Flip one byte so the request keeps its shape but fails signature verification.
    byte[] forgedSignature = validRequest.signature();
    forgedSignature[0] ^= 0x01;
    return new ClientRequest(validRequest.clientSenderId(), validRequest.requestId(), validRequest.command(), forgedSignature);
  }

  private static Path integrationConfigPath() {
    Path configPath = Path.of(System.getProperty("user.dir"), "config", "config.properties").toAbsolutePath();
    assertTrue(Files.exists(configPath), "Missing config file: " + configPath);
    return configPath;
  }

  private static void populateConfig(Path configPath) throws IOException, InterruptedException {
    ProcessResult populateResult = runPopulate(configPath);
    assertEquals(0, populateResult.exitCode(), populateResult.output());
  }

  private static List<Process> startServers(List<String> replicaIds, Path configPath) throws IOException {
    List<Process> servers = new ArrayList<>();
    for (String replicaId : replicaIds) {
      servers.add(startServer(replicaId, configPath));
    }
    return servers;
  }

  private static void waitForServersStartup(Duration duration) throws InterruptedException {
    Thread.sleep(duration.toMillis());
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
      ConfigParser.ReplicaSection replica = config.requireReplica(replicaId);
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

          Message message = SerializationUtil.decodeReplicaMessage(inbound.packet().payload());
          if (!sentInvalidVote && message.getType() == MessageType.PREPARE) {
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
      Node originalNode = message.getNode();
      if (originalNode == null) {
        return;
      }

      // Keep the sender identity, but change the node hash so the leader rejects the vote.
      Node invalidNode = new Node(originalNode.getParentHash(), originalNode.getThisHash() + "-evil", originalNode.getViewNumber(), originalNode.getCommand());
      Message invalidVote = new Message(message.getCurrView(), senderId, MessageType.PREPARE, invalidNode, null);
      transport.send(inbound.packet().connectionId(), SerializationUtil.encodeReplicaMessage(invalidVote), inbound.sender());
      invalidVotesSent.incrementAndGet();
      sentInvalidVote = true;
    }

    private void sendInitialNewView(ConfigParser config) throws Exception {
      ConfigParser.ReplicaSection leader = config.replicas().getFirst();
      InetSocketAddress leaderAddress = new InetSocketAddress(leader.host(), leader.consensusPort());
      QuorumCertificate genesisQC = new QuorumCertificate(MessageType.DECIDE, -1, Node.GENESIS_NODE);
      Message newView = new Message(0, senderId, MessageType.NEW_VIEW, Node.GENESIS_NODE, genesisQC);
      long connectionId = ThreadLocalRandom.current().nextLong();
      transport.send(connectionId, SerializationUtil.encodeReplicaMessage(newView), leaderAddress);
    }

    @Override
    public void close() throws Exception {
      running = false;
      transport.close();
      worker.join(TimeUnit.SECONDS.toMillis(1));
    }
  }

  private static Process startServer(String replicaId, Path configPath) throws IOException {
    ProcessBuilder processBuilder = new ProcessBuilder(javaExecutable(), "-cp", System.getProperty("java.class.path"), "pt.ulisboa.depchain.server.Main", replicaId,
        configPath.toString());
    processBuilder.redirectErrorStream(true);
    return processBuilder.start();
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

  private static void stopProcesses(List<Process> processes) {
    for (Process process : processes) {
      if (process.isAlive()) {
        process.destroy();
      }
    }

    for (Process process : processes) {
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

    try {
      Thread.sleep(Duration.ofSeconds(3).toMillis());
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
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
}
