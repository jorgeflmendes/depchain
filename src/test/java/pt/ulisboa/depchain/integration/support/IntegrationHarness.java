package pt.ulisboa.depchain.integration.support;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.DatagramSocket;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import pt.ulisboa.depchain.integration.byzantine.ByzantineAttackMode;
import pt.ulisboa.depchain.integration.byzantine.ByzantineReplicaServer;
import pt.ulisboa.depchain.proto.AppendRequest;
import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.ClientResponse;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.config.ConfigParser.ReplicaSection;
import pt.ulisboa.depchain.shared.keys.PrivateKeyLoader;
import pt.ulisboa.depchain.shared.keys.PublicKeyLoader;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.ClientRequestSignaturePayloadUtil;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;
import pt.ulisboa.depchain.shared.utils.ProtoValidationUtil;

public abstract class IntegrationHarness {
  protected static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(8);
  protected static final Duration STANDARD_REQUEST_TIMEOUT = Duration.ofSeconds(12);
  protected static final Duration VIEW_CHANGE_REQUEST_TIMEOUT = Duration.ofSeconds(20);
  protected static final Duration REPLAY_INITIAL_TIMEOUT = Duration.ofSeconds(8);
  protected static final Duration REPLAY_RESPONSE_TIMEOUT = Duration.ofSeconds(1);
  protected static final Duration EXPECTED_TIMEOUT = Duration.ofSeconds(5);

  protected static final String LEADER_REPLICA_ID = "server1";
  protected static final String FOLLOWER_REPLICA_ID = "server2";
  protected static final String BYZANTINE_REPLICA_ID = "server3";
  protected static final String SECOND_BYZANTINE_REPLICA_ID = "server4";
  protected static final List<String> REPLICA_IDS = List.of("server1", "server2", "server3", "server4");
  protected static final List<String> HONEST_REPLICA_IDS = List.of("server1", "server2");
  protected static final List<String> HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS = List.of("server1", "server2", "server4");
  protected static final List<String> HONEST_WITH_BYZANTINE_LEADER_REPLICA_IDS = List.of("server2", "server3", "server4");

  private static final AtomicInteger PORT_BLOCK_COUNTER = new AtomicInteger();
  private static final Pattern CONSENSUS_PORT_PATTERN = Pattern.compile("(consensus:\\s+)(\\d+)");
  private static final Pattern CLIENT_PORT_PATTERN = Pattern.compile("(client:\\s+)(\\d+)");
  private static final Pattern VIEW_CHANGE_TIMEOUT_PATTERN = Pattern.compile("(viewChangeMs:\\s+)(\\d+)");
  private static final Pattern CLIENT_COMMAND_WAIT_TIMEOUT_PATTERN = Pattern.compile("(clientCommandWaitMs:\\s+)(\\d+)");
  private static final Pattern THRESHOLD_ROUND_TIMEOUT_PATTERN = Pattern.compile("(thresholdRoundMs:\\s+)(\\d+)");
  private static final Pattern FETCH_NODE_TIMEOUT_PATTERN = Pattern.compile("(fetchNodeMs:\\s+)(\\d+)");
  private static final int INTEGRATION_VIEW_CHANGE_TIMEOUT_MS = 1500;
  private static final int INTEGRATION_CLIENT_COMMAND_WAIT_TIMEOUT_MS = 1100;
  private static final int INTEGRATION_THRESHOLD_ROUND_TIMEOUT_MS = 1100;
  private static final int INTEGRATION_FETCH_NODE_TIMEOUT_MS = 500;
  private static final Object POPULATE_LOCK = new Object();
  private static volatile boolean populated;

  protected record ProcessResult(int exitCode, String output) {
  }

  protected static Path integrationConfigPath() {
    Path baseConfigPath = Path.of(System.getProperty("user.dir"), "config", "config.yaml").toAbsolutePath();
    assertTrue(Files.exists(baseConfigPath), "Missing config file: " + baseConfigPath);
    return isolatedIntegrationConfigPath(baseConfigPath);
  }

  protected static void populateConfig(Path configPath) throws IOException, InterruptedException {
    if (populated) {
      return;
    }

    synchronized (POPULATE_LOCK) {
      if (populated) {
        return;
      }

      ProcessResult populateResult = runPopulate(configPath);
      assertEquals(0, populateResult.exitCode(), populateResult.output());
      populated = true;
    }
  }

  protected static List<StartedServer> startServers(List<String> replicaIds, Path configPath) throws IOException {
    List<StartedServer> servers = new ArrayList<>();
    for (String replicaId : replicaIds) {
      servers.add(startHonestServer(replicaId, configPath));
    }
    return servers;
  }

  protected static StartedServer startByzantineServer(String replicaId, Path configPath, ByzantineAttackMode attackMode) throws IOException {
    return startProcess(replicaId, "pt.ulisboa.depchain.integration.byzantine.ByzantineReplicaMain", configPath.toString(), attackMode.name());
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
    InboundPacket firstResponse = broadcastClientRequestPayload(configPath, payload, REPLAY_INITIAL_TIMEOUT);
    assertResponseNotNull(firstResponse, message + " (initial request)", servers);
    assertEquals("Success: " + command, decodeClientResponse(firstResponse).getAppend().getMessage(), message + " (initial request)");

    for (int i = 0; i < 3; i++) {
      InboundPacket replayResponse = broadcastClientRequestPayload(configPath, payload, REPLAY_RESPONSE_TIMEOUT);
      assertNull(replayResponse, message + " (replay " + (i + 1) + ")");
    }
  }

  protected static void assertByzantineAttackObserved(StartedServer byzantineServer, ByzantineAttackMode attackMode, String message) {
    String marker = ByzantineReplicaServer.ATTACK_MARKER + " " + attackMode;
    assertTrue(byzantineServer.outputContains(marker), message + System.lineSeparator() + byzantineServer.describeState());
  }

  protected static InboundPacket sendForgedClientRequest(Path configPath, String replicaId, String command) throws Exception {
    ConfigParser config = ConfigParser.load(configPath);
    long clientSenderId = config.client().senderId();
    PrivateKey clientPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config);
    ClientRequest forgedRequest = forgedRequest(clientSenderId, command, clientPrivateKey);
    return sendPayloadToClientPort(configPath, replicaId, clientSenderId, clientPrivateKey, PublicKeyLoader.loadStaticPublicKeys(config), ProtoValidationUtil
        .requireValid(forgedRequest, "ClientRequest").toByteArray(), Duration.ofSeconds(3));
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

  protected static ClientRequest signedAppendRequest(long clientSenderId, long requestId, String command, PrivateKey clientPrivateKey) throws Exception {
    byte[] signature = CryptoUtil.signEcdsa(ClientRequestSignaturePayloadUtil.signedAppendRequestPayload(clientSenderId, requestId, command), clientPrivateKey);
    return ProtoValidationUtil.requireValid(ClientRequest.newBuilder().setAppend(AppendRequest.newBuilder()
        .setRequestKey(ClientRequestKey.newBuilder().setClientSenderId(clientSenderId).setRequestId(requestId)).setValue(command).setSignature(ByteString.copyFrom(signature)))
        .build(), "ClientRequest");
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

  protected static InboundPacket broadcastClientRequestPayload(Path configPath, byte[] payload, Duration timeout) throws Exception {
    ConfigParser config = ConfigParser.load(configPath);
    long clientSenderId = config.client().senderId();
    PrivateKey clientPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config);
    Map<Long, PublicKey> staticPublicKeys = PublicKeyLoader.loadStaticPublicKeys(config);
    return broadcastAuthenticatedPayload(config, clientSenderId, clientPrivateKey, staticPublicKeys, payload, timeout);
  }

  protected static void stopProcesses(List<StartedServer> servers) {
    for (StartedServer server : servers) {
      stopProcess(server);
    }

    for (StartedServer server : servers) {
      Process process = server.process();
      if (!process.isAlive()) {
        continue;
      }

      try {
        if (!process.waitFor(2, TimeUnit.SECONDS)) {
          process.destroyForcibly();
          process.waitFor(2, TimeUnit.SECONDS);
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

  protected static void stopProcess(StartedServer server) {
    Process process = server.process();
    if (!process.isAlive()) {
      return;
    }

    process.destroy();
    try {
      if (!process.waitFor(2, TimeUnit.SECONDS)) {
        process.destroyForcibly();
        process.waitFor(2, TimeUnit.SECONDS);
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      process.destroyForcibly();
    }
  }

  private static Path isolatedIntegrationConfigPath(Path baseConfigPath) {
    try {
      String baseConfig = Files.readString(baseConfigPath, StandardCharsets.UTF_8);
      int configIndex = PORT_BLOCK_COUNTER.getAndIncrement();
      List<Integer> availablePorts = reserveAvailableUdpPorts(8);
      AtomicInteger portOffset = new AtomicInteger();
      String configWithConsensusPorts = rewritePorts(baseConfig, CONSENSUS_PORT_PATTERN, availablePorts, portOffset);
      String configWithClientPorts = rewritePorts(configWithConsensusPorts, CLIENT_PORT_PATTERN, availablePorts, portOffset);
      String configWithViewTimeout = rewriteScalarValue(configWithClientPorts, VIEW_CHANGE_TIMEOUT_PATTERN, INTEGRATION_VIEW_CHANGE_TIMEOUT_MS);
      String configWithClientCommandTimeout = rewriteScalarValue(configWithViewTimeout, CLIENT_COMMAND_WAIT_TIMEOUT_PATTERN, INTEGRATION_CLIENT_COMMAND_WAIT_TIMEOUT_MS);
      String configWithThresholdTimeout = rewriteScalarValue(configWithClientCommandTimeout, THRESHOLD_ROUND_TIMEOUT_PATTERN, INTEGRATION_THRESHOLD_ROUND_TIMEOUT_MS);
      String isolatedConfig = rewriteScalarValue(configWithThresholdTimeout, FETCH_NODE_TIMEOUT_PATTERN, INTEGRATION_FETCH_NODE_TIMEOUT_MS);

      Path targetDirectory = Path.of(System.getProperty("user.dir"), "target", "integration-configs");
      Files.createDirectories(targetDirectory);
      Path isolatedConfigPath = targetDirectory.resolve("config-" + configIndex + ".yaml");
      Files.writeString(isolatedConfigPath, isolatedConfig, StandardCharsets.UTF_8);
      return isolatedConfigPath;
    } catch (IOException exception) {
      throw new IllegalStateException("Could not create isolated integration config", exception);
    }
  }

  private static List<Integer> reserveAvailableUdpPorts(int count) throws IOException {
    List<DatagramSocket> reservedSockets = new ArrayList<>(count);
    try {
      for (int i = 0; i < count; i++) {
        reservedSockets.add(new DatagramSocket(0));
      }
      return reservedSockets.stream().map(DatagramSocket::getLocalPort).toList();
    } finally {
      for (DatagramSocket reservedSocket : reservedSockets) {
        reservedSocket.close();
      }
    }
  }

  private static String rewritePorts(String configContents, Pattern pattern, List<Integer> availablePorts, AtomicInteger portOffset) {
    Matcher matcher = pattern.matcher(configContents);
    StringBuffer output = new StringBuffer();
    while (matcher.find()) {
      int nextPort = availablePorts.get(portOffset.getAndIncrement());
      matcher.appendReplacement(output, Matcher.quoteReplacement(matcher.group(1) + nextPort));
    }
    matcher.appendTail(output);
    return output.toString();
  }

  private static String rewriteScalarValue(String configContents, Pattern pattern, int value) {
    Matcher matcher = pattern.matcher(configContents);
    if (!matcher.find()) {
      return configContents;
    }

    return matcher.replaceFirst(Matcher.quoteReplacement(matcher.group(1) + value));
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
    }
  }

  private static ClientRequest forgedRequest(long clientSenderId, String command, PrivateKey clientPrivateKey) throws Exception {
    long requestId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    ClientRequest validRequest = signedAppendRequest(clientSenderId, requestId, command, clientPrivateKey);
    byte[] forgedSignature = validRequest.getAppend().getSignature().toByteArray();
    forgedSignature[0] ^= 0x01;
    return validRequest.toBuilder().setAppend(validRequest.getAppend().toBuilder().setSignature(ByteString.copyFrom(forgedSignature))).build();
  }

  private static StartedServer startHonestServer(String replicaId, Path configPath) throws IOException {
    return startProcess(replicaId, "pt.ulisboa.depchain.server.Main", configPath.toString());
  }

  private static StartedServer startProcess(String replicaId, String mainClass, String... args) throws IOException {
    List<String> command = new ArrayList<>();
    command.add(javaExecutable());
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(mainClass);
    command.add(replicaId);
    for (String arg : args) {
      command.add(arg);
    }

    ProcessBuilder processBuilder = new ProcessBuilder(command);
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

  public static final class StartedServer {
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

    public Process process() {
      return process;
    }

    public boolean awaitReady(Duration timeout) throws InterruptedException {
      if (ready.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
        return process.isAlive();
      }
      return false;
    }

    public void awaitOutputDrain(Duration timeout) {
      try {
        outputReader.join(timeout.toMillis());
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
    }

    public String describeState() {
      String state = "alive";
      if (!process.isAlive()) {
        state = "exited(" + process.exitValue() + ")";
      }
      return "Server " + replicaId + " [" + state + ", clientReady=" + clientReady.get() + ", nodeReady=" + nodeReady.get() + "]" + System.lineSeparator() + outputSnapshot();
    }

    public boolean outputContains(String text) {
      synchronized (output) {
        return output.indexOf(text) >= 0;
      }
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
