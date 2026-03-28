package pt.ulisboa.depchain.integration.support;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.awaitility.core.ConditionFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import pt.ulisboa.depchain.integration.byzantine.ByzantineAttackMode;
import pt.ulisboa.depchain.integration.byzantine.ByzantineReplicaNode;
import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.ClientResponse;
import pt.ulisboa.depchain.proto.TransactionRequest;
import pt.ulisboa.depchain.proto.TransactionResponse;
import pt.ulisboa.depchain.proto.TransactionType;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.config.ConfigParser.ReplicaSection;
import pt.ulisboa.depchain.shared.crypto.ClientRequestSignaturePayloadUtil;
import pt.ulisboa.depchain.shared.crypto.CryptoUtil;
import pt.ulisboa.depchain.shared.crypto.key.PrivateKeyLoader;
import pt.ulisboa.depchain.shared.crypto.key.PublicKeyLoader;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.validation.ProtoValidationUtil;
import pt.ulisboa.depchain.testsupport.TestKeyMaterialSupport;

public abstract class IntegrationHarness {
  protected static final Duration AWAIT_POLL_INTERVAL = Duration.ofMillis(50);
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
  protected static final String TEST_RECIPIENT_ADDRESS = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
  protected static final long TEST_TRANSFER_AMOUNT = 1L;
  protected static final long TEST_GAS_LIMIT = 21_000L;
  protected static final long TEST_GAS_PRICE = 1L;

  private static final AtomicInteger PORT_BLOCK_COUNTER = new AtomicInteger();
  private static final Map<Path, AtomicLong> NEXT_NONCE_BY_CONFIG = new ConcurrentHashMap<>();
  private static final Pattern CONSENSUS_PORT_PATTERN = Pattern.compile("(consensus:\\s+)(\\d+)");
  private static final Pattern CLIENT_PORT_PATTERN = Pattern.compile("(client:\\s+)(\\d+)");
  private static final Pattern KEYS_ROOT_PATTERN = Pattern.compile("(?m)^(\\s*root:\\s+).*$");
  private static final Pattern BLOCKS_ROOT_PATTERN = Pattern.compile("(?m)^(\\s*blocksRoot:\\s+).*$");
  protected static Path integrationConfigPath() {
    Path baseConfigPath = projectConfigPath();
    return isolatedIntegrationConfigPath(baseConfigPath);
  }

  protected static Path projectConfigPath() {
    Path baseConfigPath = TestKeyMaterialSupport.projectConfigPath();
    assertTrue(Files.exists(baseConfigPath), "Missing config file: " + baseConfigPath);
    return baseConfigPath;
  }

  protected static void populateConfig(Path configPath) throws IOException, InterruptedException {
    TestKeyMaterialSupport.ensureKeyMaterial(configPath);
  }

  protected static List<StartedServer> startServers(List<String> replicaIds, Path configPath) throws IOException {
    return startServers(replicaIds, configPath, Map.of());
  }

  protected static List<StartedServer> startServers(List<String> replicaIds, Path configPath, Map<String, String> jvmProperties) throws IOException {
    List<StartedServer> servers = new ArrayList<>();
    for (String replicaId : replicaIds) {
      servers.add(startHonestServer(replicaId, configPath, jvmProperties));
    }
    return servers;
  }

  protected static List<StartedServer> startServersWithPerReplicaJvmProperties(List<String> replicaIds, Path configPath, Map<String, Map<String, String>> jvmPropertiesByReplicaId)
      throws IOException {
    List<StartedServer> servers = new ArrayList<>();
    for (String replicaId : replicaIds) {
      Map<String, String> jvmProperties = jvmPropertiesByReplicaId.getOrDefault(replicaId, Map.of());
      servers.add(startHonestServer(replicaId, configPath, jvmProperties));
    }
    return servers;
  }

  protected static void cleanPersistedBlockData(Path configPath) throws IOException {
    ConfigParser config = ConfigParser.load(configPath);
    Path blocksRoot = config.storage().blocksRootPath();
    NEXT_NONCE_BY_CONFIG.remove(normalizedConfigPath(configPath));
    if (!Files.exists(blocksRoot)) {
      return;
    }

    try (var paths = Files.walk(blocksRoot)) {
      paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
        try {
          Files.deleteIfExists(path);
        } catch (IOException exception) {
          throw new IllegalStateException("Could not clean block path " + path, exception);
        }
      });
    }
  }

  protected static void cleanupIsolatedConfigDirectory(Path configPath) throws IOException {
    Path normalizedConfigPath = normalizedConfigPath(configPath);
    NEXT_NONCE_BY_CONFIG.remove(normalizedConfigPath);

    Path configDirectory = normalizedConfigPath.getParent();
    if (configDirectory == null || !isGeneratedTestConfigDirectory(configDirectory)) {
      return;
    }

    deleteDirectoryRecursively(configDirectory);
  }

  protected static StartedServer startByzantineServer(String replicaId, Path configPath, ByzantineAttackMode attackMode) throws IOException {
    return startProcess(replicaId, "pt.ulisboa.depchain.integration.byzantine.ByzantineReplicaApplication", configPath.toString(), attackMode.name());
  }

  protected static ManagedCluster startManagedCluster(List<String> replicaIds) throws Exception {
    return startManagedCluster(replicaIds, Map.of());
  }

  protected static ManagedCluster startManagedCluster(List<String> replicaIds, Map<String, String> jvmProperties) throws Exception {
    Path configPath = integrationConfigPath();
    cleanPersistedBlockData(configPath);
    populateConfig(configPath);

    List<StartedServer> servers = startServers(replicaIds, configPath, jvmProperties);
    try {
      waitForServersStartup(servers, STARTUP_TIMEOUT);
      return new ManagedCluster(configPath, servers, TestClientSession.connect(configPath));
    } catch (Exception exception) {
      try {
        stopProcesses(servers);
      } finally {
        try {
          cleanPersistedBlockData(configPath);
        } finally {
          cleanupIsolatedConfigDirectory(configPath);
        }
      }
      throw exception;
    }
  }

  protected static ManagedCluster startManagedClusterWithPerReplicaJvmProperties(List<String> replicaIds, Map<String, Map<String, String>> jvmPropertiesByReplicaId)
      throws Exception {
    Path configPath = integrationConfigPath();
    cleanPersistedBlockData(configPath);
    populateConfig(configPath);

    List<StartedServer> servers = startServersWithPerReplicaJvmProperties(replicaIds, configPath, jvmPropertiesByReplicaId);
    try {
      waitForServersStartup(servers, STARTUP_TIMEOUT);
      return new ManagedCluster(configPath, servers, TestClientSession.connect(configPath));
    } catch (Exception exception) {
      try {
        stopProcesses(servers);
      } finally {
        try {
          cleanPersistedBlockData(configPath);
        } finally {
          cleanupIsolatedConfigDirectory(configPath);
        }
      }
      throw exception;
    }
  }

  protected static void waitForServersStartup(List<StartedServer> servers, Duration timeout) throws InterruptedException {
    for (StartedServer server : servers) {
      awaitFor("server startup for " + server.replicaId()).atMost(timeout).until(server::isReady);
    }
  }

  protected static void assertRequestSucceeds(Path configPath, String command, Duration timeout, List<StartedServer> servers, String message) throws Exception {
    ClientRequest request = signedRequest(configPath, command);
    byte[] payload = ProtoValidationUtil.requireValid(request, "ClientRequest").toByteArray();
    InboundPacket response = broadcastClientRequestPayload(configPath, payload, timeout);
    assertResponseNotNull(response, message, servers);
    assertSuccessfulTransactionResponse(decodeClientResponse(response), message);
  }

  protected static void assertByzantineAttackObserved(StartedServer byzantineServer, ByzantineAttackMode attackMode, String message) {
    String marker = ByzantineReplicaNode.ATTACK_MARKER + " " + attackMode;
    awaitFor("byzantine attack marker " + attackMode).atMost(EXPECTED_TIMEOUT).until(() -> byzantineServer.outputContains(marker));
    assertTrue(byzantineServer.outputContains(marker), message + System.lineSeparator() + byzantineServer.describeState());
  }

  protected static ClientRequest signedRequest(Path configPath, String command) throws Exception {
    ConfigParser config = ConfigParser.load(configPath);
    long clientSenderId = config.client().senderId();
    PrivateKey clientPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config);
    long requestId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    return signedTransferRequest(clientSenderId, requestId, nextNonce(configPath), clientPrivateKey);
  }

  protected static ClientRequest signedTransferRequest(long clientSenderId, long requestId, long nonce, PrivateKey clientPrivateKey) throws Exception {
    byte[] signature = CryptoUtil.signEcdsa(ClientRequestSignaturePayloadUtil
        .signedTransactionRequestPayload(clientSenderId, requestId, TransactionType.TRANSACTION_TYPE_TRANSFER, TEST_RECIPIENT_ADDRESS, TEST_TRANSFER_AMOUNT, nonce, TEST_GAS_LIMIT, TEST_GAS_PRICE), clientPrivateKey);
    return ProtoValidationUtil.requireValid(ClientRequest.newBuilder()
        .setTransaction(TransactionRequest.newBuilder().setRequestKey(ClientRequestKey.newBuilder().setClientSenderId(clientSenderId).setRequestId(requestId))
            .setType(TransactionType.TRANSACTION_TYPE_TRANSFER).setTo(TEST_RECIPIENT_ADDRESS).setAmount(TEST_TRANSFER_AMOUNT).setNonce(nonce).setGasLimit(TEST_GAS_LIMIT)
            .setGasPrice(TEST_GAS_PRICE).setSignature(ByteString.copyFrom(signature)))
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
      return ProtoValidationUtil.requireValid(ClientResponse.parseFrom(response.payload()), "ClientResponse");
    } catch (InvalidProtocolBufferException exception) {
      throw new IllegalArgumentException("Invalid protobuf client response payload", exception);
    }
  }

  private static void assertSuccessfulTransactionResponse(ClientResponse response, String message) {
    assertTrue(response.hasTransaction(), message + " (expected transaction response)");
    TransactionResponse transaction = response.getTransaction();
    assertTrue(transaction.hasReceipt(), message + " (missing transaction receipt)");
    assertTrue(transaction.getReceipt().getSuccess(), message + " (transaction should succeed)");
  }

  private static long nextNonce(Path configPath) {
    return NEXT_NONCE_BY_CONFIG.computeIfAbsent(normalizedConfigPath(configPath), ignored -> new AtomicLong()).getAndIncrement();
  }

  private static Path normalizedConfigPath(Path configPath) {
    return configPath.toAbsolutePath().normalize();
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
        if (!awaitProcessExit(process, Duration.ofSeconds(2))) {
          process.destroyForcibly();
          awaitProcessExit(process, Duration.ofSeconds(2));
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
      if (!awaitProcessExit(process, Duration.ofSeconds(2))) {
        process.destroyForcibly();
        awaitProcessExit(process, Duration.ofSeconds(2));
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      process.destroyForcibly();
    }
  }

  private static Path isolatedIntegrationConfigPath(Path baseConfigPath) {
    try {
      String isolatedConfig = Files.readString(baseConfigPath, StandardCharsets.UTF_8);
      int configIndex = PORT_BLOCK_COUNTER.getAndIncrement();
      List<Integer> availablePorts = reserveAvailableUdpPorts(8);
      Path isolatedConfigDirectory = Files.createTempDirectory("depchain-it-config-" + configIndex + "-");
      Files.createDirectories(isolatedConfigDirectory);
      Path isolatedRuntimeDirectory = isolatedConfigDirectory.resolve("runtime");
      isolatedConfig = rewritePath(isolatedConfig, KEYS_ROOT_PATTERN, isolatedRuntimeDirectory.resolve("keys"));
      isolatedConfig = rewritePath(isolatedConfig, BLOCKS_ROOT_PATTERN, isolatedRuntimeDirectory.resolve("storage"));
      AtomicInteger portOffset = new AtomicInteger();
      isolatedConfig = rewritePorts(isolatedConfig, CONSENSUS_PORT_PATTERN, availablePorts, portOffset);
      isolatedConfig = rewritePorts(isolatedConfig, CLIENT_PORT_PATTERN, availablePorts, portOffset);
      Path isolatedConfigPath = isolatedConfigDirectory.resolve("config.yaml");
      Files.writeString(isolatedConfigPath, isolatedConfig, StandardCharsets.UTF_8);
      Path baseGenesisPath = baseConfigPath.getParent().resolve("genesis.json");
      Files.copy(baseGenesisPath, isolatedConfigDirectory.resolve("genesis.json"), StandardCopyOption.REPLACE_EXISTING);
      Path baseGenesisLockPath = baseConfigPath.getParent().resolve("genesis.lock.json");
      if (Files.exists(baseGenesisLockPath)) {
        Files.copy(baseGenesisLockPath, isolatedConfigDirectory.resolve("genesis.lock.json"), StandardCopyOption.REPLACE_EXISTING);
      }
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

  private static String rewritePath(String configContents, Pattern pattern, Path path) {
    String replacement = "$1" + "\"" + normalizePath(path) + "\"";
    String updated = pattern.matcher(configContents).replaceFirst(replacement);
    if (updated.equals(configContents)) {
      throw new IllegalStateException("Could not rewrite config path using pattern " + pattern);
    }
    return updated;
  }

  private static String normalizePath(Path path) {
    return path.toAbsolutePath().normalize().toString().replace('\\', '/');
  }

  private static boolean isGeneratedTestConfigDirectory(Path directory) {
    String directoryName = directory.getFileName() == null ? "" : directory.getFileName().toString();
    return directoryName.startsWith("depchain-it-config-") || directoryName.startsWith("depchain-test-");
  }

  private static void deleteDirectoryRecursively(Path directory) throws IOException {
    if (!Files.exists(directory)) {
      return;
    }

    try (var paths = Files.walk(directory)) {
      paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
        try {
          Files.deleteIfExists(path);
        } catch (IOException exception) {
          throw new IllegalStateException("Could not delete temporary test path " + path, exception);
        }
      });
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

      Set<Long> expectedReplicaSenderIds = PublicKeyLoader.loadReplicaPublicKeys(config).keySet();
      Map<Long, String> responseKeyByReplicaSender = new HashMap<>();
      Map<String, Integer> replyCounts = new HashMap<>();
      AtomicReference<InboundPacket> coherentResponseRef = new AtomicReference<>();

      try {
        awaitFor("coherent client response").atMost(timeout).until(() -> {
          InboundPacket inbound = transport.receive(Math.max(1L, timeout.toMillis()));
          if (inbound == null || !endpointsByConnectionId.containsKey(inbound.packet().getConnectionId())) {
            return false;
          }

          Long authenticatedSenderId = inbound.authenticatedSenderId();
          if (authenticatedSenderId == null || !expectedReplicaSenderIds.contains(authenticatedSenderId) || responseKeyByReplicaSender.containsKey(authenticatedSenderId)) {
            return false;
          }

          ClientResponse response = decodeClientResponse(inbound);
          String responseKey = Base64.getEncoder().encodeToString(ProtoValidationUtil.requireValid(response, "ClientResponse").toByteArray());
          responseKeyByReplicaSender.put(authenticatedSenderId, responseKey);

          int replyCount = replyCounts.merge(responseKey, 1, Integer::sum);
          if (replyCount >= requiredReplyCount) {
            coherentResponseRef.set(inbound);
            return true;
          }
          return false;
        });
      } catch (org.awaitility.core.ConditionTimeoutException ignored) {
        return null;
      }

      return coherentResponseRef.get();
    }
  }

  private static ClientRequest forgedRequest(long clientSenderId, String command, PrivateKey clientPrivateKey) throws Exception {
    ClientRequest validRequest = signedTransferRequest(clientSenderId, ThreadLocalRandom.current().nextLong(Long.MAX_VALUE), 991L, clientPrivateKey);
    byte[] forgedSignature = validRequest.getTransaction().getSignature().toByteArray();
    forgedSignature[0] ^= 0x01;
    return validRequest.toBuilder().setTransaction(validRequest.getTransaction().toBuilder().setSignature(ByteString.copyFrom(forgedSignature))).build();
  }

  private static StartedServer startHonestServer(String replicaId, Path configPath) throws IOException {
    return startHonestServer(replicaId, configPath, Map.of());
  }

  private static StartedServer startHonestServer(String replicaId, Path configPath, Map<String, String> jvmProperties) throws IOException {
    return startProcess(replicaId, "pt.ulisboa.depchain.server.bootstrap.ServerApplication", jvmProperties, configPath.toString());
  }

  private static StartedServer startProcess(String replicaId, String mainClass, String... args) throws IOException {
    return startProcess(replicaId, mainClass, Map.of(), args);
  }

  private static StartedServer startProcess(String replicaId, String mainClass, Map<String, String> jvmProperties, String... args) throws IOException {
    List<String> command = new ArrayList<>();
    command.add(javaExecutable());
    for (Map.Entry<String, String> entry : jvmProperties.entrySet()) {
      command.add("-D" + entry.getKey() + "=" + entry.getValue());
    }
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

  private static String javaExecutable() {
    String javaHome = System.getProperty("java.home");
    String suffix = "";
    if (System.getProperty("os.name").toLowerCase().contains("win")) {
      suffix = ".exe";
    }
    return Path.of(javaHome, "bin", "java" + suffix).toString();
  }

  private static boolean awaitProcessExit(Process process, Duration timeout) throws InterruptedException {
    try {
      awaitFor("process exit").atMost(timeout).until(() -> !process.isAlive());
      return true;
    } catch (org.awaitility.core.ConditionTimeoutException ignored) {
      return false;
    }
  }

  public static final class ManagedCluster implements AutoCloseable {
    private final Path configPath;
    private final List<StartedServer> servers;
    private final TestClientSession clientSession;

    private ManagedCluster(Path configPath, List<StartedServer> servers, TestClientSession clientSession) {
      this.configPath = configPath;
      this.servers = List.copyOf(servers);
      this.clientSession = clientSession;
    }

    public Path configPath() {
      return configPath;
    }

    public List<StartedServer> servers() {
      return servers;
    }

    public long clientSenderId() {
      return clientSession.clientSenderId();
    }

    public PrivateKey clientPrivateKey() {
      return clientSession.clientPrivateKey();
    }

    public Map<Long, PublicKey> staticPublicKeys() {
      return clientSession.staticPublicKeys();
    }

    public Map<Long, PublicKey> replicaPublicKeys() {
      return clientSession.replicaPublicKeys();
    }

    public ClientRequest signedRequest(String command) throws Exception {
      return clientSession.signedRequest(command);
    }

    public void assertRequestSucceeds(String command, Duration timeout, String message) throws Exception {
      ClientRequest request = clientSession.signedRequest(command);
      byte[] payload = ProtoValidationUtil.requireValid(request, "ClientRequest").toByteArray();
      InboundPacket response = clientSession.broadcastPayload(payload, timeout);
      assertResponseNotNull(response, message, servers);
      assertSuccessfulTransactionResponse(decodeClientResponse(response), message);
    }

    public void assertReplayIsIgnored(String command, String message) throws Exception {
      ClientRequest request = clientSession.signedRequest(command);
      byte[] payload = ProtoValidationUtil.requireValid(request, "ClientRequest").toByteArray();
      InboundPacket firstResponse = clientSession.broadcastPayload(payload, REPLAY_INITIAL_TIMEOUT);
      assertResponseNotNull(firstResponse, message + " (initial request)", servers);
      assertSuccessfulTransactionResponse(decodeClientResponse(firstResponse), message + " (initial request)");

      for (int i = 0; i < 3; i++) {
        InboundPacket replayResponse = clientSession.broadcastPayload(payload, REPLAY_RESPONSE_TIMEOUT);
        assertNull(replayResponse, message + " (replay " + (i + 1) + ")");
      }
    }

    public InboundPacket sendForgedClientRequest(String replicaId, String command) throws Exception {
      ClientRequest forgedRequest = forgedRequest(clientSession.clientSenderId(), command, clientSession.clientPrivateKey());
      byte[] payload = ProtoValidationUtil.requireValid(forgedRequest, "ClientRequest").toByteArray();
      return clientSession.sendPayloadToClientPort(replicaId, payload, Duration.ofSeconds(3));
    }

    public InboundPacket sendPayloadToClientPort(String replicaId, byte[] payload, Duration timeout) throws Exception {
      return clientSession.sendPayloadToClientPort(replicaId, payload, timeout);
    }

    public InboundPacket sendPayloadToConsensusPort(String replicaId, byte[] payload, Duration timeout) throws Exception {
      return clientSession.sendPayloadToConsensusPort(replicaId, payload, timeout);
    }

    @Override
    public void close() throws Exception {
      try {
        clientSession.close();
      } finally {
        try {
          stopProcesses(servers);
        } finally {
          try {
            cleanPersistedBlockData(configPath);
          } finally {
            cleanupIsolatedConfigDirectory(configPath);
          }
        }
      }
    }
  }

  private static final class TestClientSession implements AutoCloseable {
    private final ConfigParser config;
    private final long clientSenderId;
    private final PrivateKey clientPrivateKey;
    private final Map<Long, PublicKey> staticPublicKeys;
    private final Map<Long, PublicKey> replicaPublicKeys;
    private final AuthenticatedLink transport;
    private final AtomicLong nextNonce;

    private TestClientSession(ConfigParser config, long clientSenderId, PrivateKey clientPrivateKey, Map<Long, PublicKey> staticPublicKeys, Map<Long, PublicKey> replicaPublicKeys,
        AuthenticatedLink transport, AtomicLong nextNonce) {
      this.config = config;
      this.clientSenderId = clientSenderId;
      this.clientPrivateKey = clientPrivateKey;
      this.staticPublicKeys = staticPublicKeys;
      this.replicaPublicKeys = replicaPublicKeys;
      this.transport = transport;
      this.nextNonce = nextNonce;
    }

    static TestClientSession connect(Path configPath) throws Exception {
      ConfigParser config = ConfigParser.load(configPath);
      long clientSenderId = config.client().senderId();
      PrivateKey clientPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config);
      Map<Long, PublicKey> staticPublicKeys = PublicKeyLoader.loadStaticPublicKeys(config);
      Map<Long, PublicKey> replicaPublicKeys = PublicKeyLoader.loadReplicaPublicKeys(config);
      AuthenticatedLink transport = AuthenticatedLink.unbound(clientSenderId, clientPrivateKey, staticPublicKeys);
      return new TestClientSession(config, clientSenderId, clientPrivateKey, staticPublicKeys, replicaPublicKeys, transport, new AtomicLong());
    }

    long clientSenderId() {
      return clientSenderId;
    }

    PrivateKey clientPrivateKey() {
      return clientPrivateKey;
    }

    Map<Long, PublicKey> staticPublicKeys() {
      return staticPublicKeys;
    }

    Map<Long, PublicKey> replicaPublicKeys() {
      return replicaPublicKeys;
    }

    ClientRequest signedRequest(String command) throws Exception {
      long requestId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
      return signedTransferRequest(clientSenderId, requestId, nextNonce.getAndIncrement(), clientPrivateKey);
    }

    InboundPacket sendPayloadToClientPort(String replicaId, byte[] payload, Duration timeout) throws Exception {
      ReplicaSection targetReplica = config.requireReplicaById(replicaId);
      InetSocketAddress targetAddress = new InetSocketAddress(targetReplica.host(), targetReplica.clientPort());
      return sendPayload(targetAddress, payload, timeout);
    }

    InboundPacket sendPayloadToConsensusPort(String replicaId, byte[] payload, Duration timeout) throws Exception {
      ReplicaSection targetReplica = config.requireReplicaById(replicaId);
      InetSocketAddress targetAddress = new InetSocketAddress(targetReplica.host(), targetReplica.consensusPort());
      return sendPayload(targetAddress, payload, timeout);
    }

    InboundPacket broadcastPayload(byte[] payload, Duration timeout) throws Exception {
      Map<Long, InetSocketAddress> endpointsByConnectionId = new LinkedHashMap<>();
      int requiredReplyCount = config.system().f() + 1;

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

      Map<Long, String> responseKeyByReplicaSender = new HashMap<>();
      Map<String, Integer> replyCounts = new HashMap<>();
      AtomicReference<InboundPacket> coherentResponseRef = new AtomicReference<>();

      try {
        try {
          awaitFor("payload response").atMost(timeout).until(() -> {
            InboundPacket inbound = transport.receive(Math.max(1L, timeout.toMillis()));
            if (inbound == null || !endpointsByConnectionId.containsKey(inbound.packet().getConnectionId())) {
              return false;
            }

            Long authenticatedSenderId = inbound.authenticatedSenderId();
            if (authenticatedSenderId == null || !replicaPublicKeys.containsKey(authenticatedSenderId) || responseKeyByReplicaSender.containsKey(authenticatedSenderId)) {
              return false;
            }

            ClientResponse response = decodeClientResponse(inbound);
            String responseKey = Base64.getEncoder().encodeToString(ProtoValidationUtil.requireValid(response, "ClientResponse").toByteArray());
            responseKeyByReplicaSender.put(authenticatedSenderId, responseKey);

            int replyCount = replyCounts.merge(responseKey, 1, Integer::sum);
            if (replyCount >= requiredReplyCount) {
              coherentResponseRef.set(inbound);
              return true;
            }
            return false;
          });
        } catch (org.awaitility.core.ConditionTimeoutException ignored) {
          return null;
        }

        return coherentResponseRef.get();
      } finally {
        closeConnections(endpointsByConnectionId);
      }
    }

    @Override
    public void close() throws Exception {
      transport.close();
    }

    private InboundPacket sendPayload(InetSocketAddress targetAddress, byte[] payload, Duration timeout) throws Exception {
      long connectionId = ThreadLocalRandom.current().nextLong();
      transport.send(connectionId, payload, targetAddress);

      try {
        AtomicReference<InboundPacket> responseRef = new AtomicReference<>();
        try {
          await().atMost(timeout).until(() -> {
            InboundPacket inbound = transport.receive(Math.max(1L, timeout.toMillis()));
            if (inbound != null && inbound.packet().getConnectionId() == connectionId) {
              responseRef.set(inbound);
              return true;
            }
            return false;
          });
        } catch (org.awaitility.core.ConditionTimeoutException ignored) {
          return null;
        }

        return responseRef.get();
      } finally {
        closeConnection(connectionId, targetAddress);
      }
    }

    private void closeConnections(Map<Long, InetSocketAddress> endpointsByConnectionId) {
      for (Map.Entry<Long, InetSocketAddress> entry : endpointsByConnectionId.entrySet()) {
        closeConnection(entry.getKey(), entry.getValue());
      }
    }

    private void closeConnection(long connectionId, InetSocketAddress targetAddress) {
      try {
        transport.closeConnection(connectionId, targetAddress);
      } catch (RuntimeException ignored) {
      }
    }
  }

  protected static ConditionFactory awaitFor(String alias) {
    return await().alias(alias).pollDelay(Duration.ZERO).pollInterval(AWAIT_POLL_INTERVAL);
  }

  protected static ConditionFactory awaitFor() {
    return await().pollDelay(Duration.ZERO).pollInterval(AWAIT_POLL_INTERVAL);
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

    public String replicaId() {
      return replicaId;
    }

    public boolean isReady() {
      return ready.getCount() == 0L && process.isAlive();
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
