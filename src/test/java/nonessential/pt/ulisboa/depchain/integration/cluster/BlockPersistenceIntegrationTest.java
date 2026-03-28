package pt.ulisboa.depchain.integration.cluster;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.google.protobuf.ByteString;

import pt.ulisboa.depchain.integration.support.IntegrationHarness;
import pt.ulisboa.depchain.integration.support.IntegrationHarness.StartedServer;
import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.ClientResponse;
import pt.ulisboa.depchain.proto.TransactionRequest;
import pt.ulisboa.depchain.proto.TransactionType;
import pt.ulisboa.depchain.server.node.BlockStore;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.crypto.ClientRequestSignaturePayloadUtil;
import pt.ulisboa.depchain.shared.crypto.CryptoUtil;
import pt.ulisboa.depchain.shared.crypto.key.PrivateKeyLoader;
import pt.ulisboa.depchain.shared.crypto.key.PublicKeyLoader;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.validation.ProtoValidationUtil;

@Tag("integration")
class BlockPersistenceIntegrationTest extends IntegrationHarness {
  private static final long TRANSFER_GAS_LIMIT = 21_000L;
  private static final long TRANSFER_GAS_PRICE = 1L;
  private static final long LARGE_TRANSFER_GAS_LIMIT = 20_000_000L;
  private static final String RECIPIENT = "cccccccccccccccccccccccccccccccccccccccc";
  private static final long TRANSFER_AMOUNT = 7L;
  private static final long SECOND_TRANSFER_AMOUNT = 5L;

  private Path configPath;

  @BeforeEach
  void cleanBeforeTest() throws IOException {
    configPath = integrationConfigPath();
    cleanPersistedBlockData(configPath);
  }

  @AfterEach
  void cleanAfterTest() throws IOException {
    if (configPath != null) {
      try {
        cleanPersistedBlockData(configPath);
      } finally {
        cleanupIsolatedConfigDirectory(configPath);
      }
    }
  }

  @Test
  @Timeout(60)
  void transactionExecutionPersistsBlockWithTransactionAndUpdatedStatePerReplica() throws Exception {
    populateConfig(configPath);

    List<StartedServer> servers = startServers(HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS, configPath);
    try {
      waitForServersStartupWithDiagnostics(servers, Duration.ofSeconds(35));

      ClientRequest transfer = signedTransferRequest(configPath, RECIPIENT, TRANSFER_AMOUNT, 0L, TRANSFER_GAS_LIMIT, TRANSFER_GAS_PRICE);
      byte[] payload = ProtoValidationUtil.requireValid(transfer, "ClientRequest").toByteArray();

      var responsePacket = broadcastClientRequestPayload(configPath, payload, STANDARD_REQUEST_TIMEOUT);
      assertAcceptedTransactionResponse(responsePacket, "Transaction request should receive a response", servers);

      await().atMost(Duration.ofSeconds(12)).untilAsserted(() -> assertPersistedTransferForReplicas(HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS, TRANSFER_AMOUNT, 0L, 1L));
    } finally {
      stopProcesses(servers);
    }
  }

  @Test
  @Timeout(90)
  void persistedStateIsRecoveredAfterClusterRestart() throws Exception {
    populateConfig(configPath);

    List<StartedServer> servers = startServers(HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS, configPath);
    try {
      waitForServersStartupWithDiagnostics(servers, Duration.ofSeconds(35));

      byte[] firstPayload = ProtoValidationUtil
          .requireValid(signedTransferRequest(configPath, RECIPIENT, TRANSFER_AMOUNT, 0L, TRANSFER_GAS_LIMIT, TRANSFER_GAS_PRICE), "ClientRequest").toByteArray();
      var firstResponse = broadcastClientRequestPayload(configPath, firstPayload, STANDARD_REQUEST_TIMEOUT);
      assertAcceptedTransactionResponse(firstResponse, "First transaction request should receive a response", servers);

      await().atMost(Duration.ofSeconds(12)).untilAsserted(() -> assertPersistedTransferForReplicas(HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS, TRANSFER_AMOUNT, 0L, 1L));
    } finally {
      stopProcesses(servers);
    }

    List<StartedServer> restartedServers = startServers(HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS, configPath);
    try {
      waitForServersStartupWithDiagnostics(restartedServers, Duration.ofSeconds(35));

      byte[] secondPayload = ProtoValidationUtil
          .requireValid(signedTransferRequest(configPath, RECIPIENT, SECOND_TRANSFER_AMOUNT, 1L, TRANSFER_GAS_LIMIT, TRANSFER_GAS_PRICE), "ClientRequest").toByteArray();
      var secondResponse = broadcastClientRequestPayload(configPath, secondPayload, STANDARD_REQUEST_TIMEOUT);
      assertAcceptedTransactionResponse(secondResponse, "Transaction request after restart should receive a response", restartedServers);

      await().atMost(Duration.ofSeconds(12)).untilAsserted(() -> {
        ConfigParser config = ConfigParser.load(configPath);
        for (String replicaId : HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS) {
          BlockStore.BlockDocument latest = BlockStore.forReplica(config, replicaId).loadLatest()
              .orElseThrow(() -> new AssertionError("Latest block was not persisted for " + replicaId));
          assertEquals(2L, latest.height(), "Recovered replica should append a second block after restart");
          assertTrue(!latest.transactions().isEmpty(), "Recovered block should include the second transfer transaction");
          assertEquals(1L, latest.transactions().getFirst().nonce(), "Recovered cluster should preserve the client nonce across restart");
          assertEquals(Long.toString(TRANSFER_AMOUNT + SECOND_TRANSFER_AMOUNT), latest.state().get(RECIPIENT)
              .balance(), "Recovered state should accumulate balances across restarts");
        }
      });
    } finally {
      stopProcesses(restartedServers);
    }
  }

  @Test
  @Timeout(90)
  void multipleClientTransactionsArePersistedInSingleBlockOrderedByGasPrice() throws Exception {
    populateConfig(configPath);

    List<StartedServer> servers = startServers(HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS, configPath);
    try {
      waitForServersStartupWithDiagnostics(servers, Duration.ofSeconds(35));

      List<ClientRequest> requests = List
          .of(signedTransferRequest(configPath, 100L, RECIPIENT, 3L, 0L, TRANSFER_GAS_LIMIT, 1L), signedTransferRequest(configPath, 101L, RECIPIENT, 11L, 0L, TRANSFER_GAS_LIMIT, 9L), signedTransferRequest(configPath, 102L, RECIPIENT, 7L, 0L, TRANSFER_GAS_LIMIT, 5L));

      try (BatchSubmission batchSubmission = submitRequestsToLeaderWithoutWaiting(configPath, requests)) {
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertPersistedBatchForReplicas(HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS));
      }
    } finally {
      stopProcesses(servers);
    }
  }

  @Test
  @Timeout(90)
  void equalGasPriceBatchUsesDeterministicClientSenderTieBreak() throws Exception {
    populateConfig(configPath);

    List<StartedServer> servers = startServers(HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS, configPath);
    try {
      waitForServersStartupWithDiagnostics(servers, Duration.ofSeconds(35));

      List<ClientRequest> requests = List
          .of(signedTransferRequest(configPath, 102L, 202L, RECIPIENT, 3L, 0L, TRANSFER_GAS_LIMIT, 4L), signedTransferRequest(configPath, 100L, 200L, RECIPIENT, 5L, 0L, TRANSFER_GAS_LIMIT, 4L), signedTransferRequest(configPath, 101L, 201L, RECIPIENT, 7L, 0L, TRANSFER_GAS_LIMIT, 4L));

      try (BatchSubmission batchSubmission = submitRequestsToLeaderWithoutWaiting(configPath, requests)) {
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertPersistedEqualGasTieBreakForReplicas(HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS));
      }
    } finally {
      stopProcesses(servers);
    }
  }

  @Test
  @Timeout(90)
  void oversizedBatchIsSplitAcrossSuccessiveBlocks() throws Exception {
    populateConfig(configPath);

    List<StartedServer> servers = startServers(HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS, configPath);
    try {
      waitForServersStartupWithDiagnostics(servers, Duration.ofSeconds(35));

      List<ClientRequest> requests = List
          .of(signedTransferRequest(configPath, 100L, 301L, RECIPIENT, 2L, 0L, LARGE_TRANSFER_GAS_LIMIT, 2L), signedTransferRequest(configPath, 100L, 302L, RECIPIENT, 4L, 1L, LARGE_TRANSFER_GAS_LIMIT, 1L));

      try (BatchSubmission batchSubmission = submitRequestsToLeaderWithoutWaiting(configPath, requests)) {
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertPersistedSplitBatchForReplicas(HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS));
      }
    } finally {
      stopProcesses(servers);
    }
  }

  @Test
  @Timeout(90)
  void equalGasPriceBatchUsesRequestIdOrderingWithinSameSender() throws Exception {
    populateConfig(configPath);

    List<StartedServer> servers = startServers(HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS, configPath);
    try {
      waitForServersStartupWithDiagnostics(servers, Duration.ofSeconds(35));

      List<ClientRequest> requests = List
          .of(signedTransferRequest(configPath, 100L, 401L, RECIPIENT, 2L, 0L, TRANSFER_GAS_LIMIT, 4L), signedTransferRequest(configPath, 100L, 402L, RECIPIENT, 4L, 1L, TRANSFER_GAS_LIMIT, 4L), signedTransferRequest(configPath, 101L, 403L, RECIPIENT, 6L, 0L, TRANSFER_GAS_LIMIT, 4L));

      try (BatchSubmission batchSubmission = submitRequestsToLeaderWithoutWaiting(configPath, requests)) {
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertPersistedSameSenderRequestIdTieBreakForReplicas(HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS));
      }
    } finally {
      stopProcesses(servers);
    }
  }

  private static ClientRequest signedTransferRequest(Path configPath, String to, long amount, long nonce, long gasLimit, long gasPrice) throws Exception {
    ConfigParser config = ConfigParser.load(configPath);
    return signedTransferRequest(configPath, config.client().senderId(), ThreadLocalRandom.current().nextLong(Long.MAX_VALUE), to, amount, nonce, gasLimit, gasPrice);
  }

  private static ClientRequest signedTransferRequest(Path configPath, long clientSenderId, String to, long amount, long nonce, long gasLimit, long gasPrice) throws Exception {
    return signedTransferRequest(configPath, clientSenderId, ThreadLocalRandom.current().nextLong(Long.MAX_VALUE), to, amount, nonce, gasLimit, gasPrice);
  }

  private static ClientRequest signedTransferRequest(Path configPath, long clientSenderId, long requestId, String to, long amount, long nonce, long gasLimit, long gasPrice)
      throws Exception {
    ConfigParser config = ConfigParser.load(configPath);
    PrivateKey clientPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config, clientSenderId);

    byte[] signaturePayload = ClientRequestSignaturePayloadUtil
        .signedTransactionRequestPayload(clientSenderId, requestId, TransactionType.TRANSACTION_TYPE_TRANSFER, to, amount, nonce, gasLimit, gasPrice);
    byte[] signature = CryptoUtil.signEcdsa(signaturePayload, clientPrivateKey);

    return ClientRequest.newBuilder()
        .setTransaction(TransactionRequest.newBuilder().setRequestKey(ClientRequestKey.newBuilder().setClientSenderId(clientSenderId).setRequestId(requestId))
            .setType(TransactionType.TRANSACTION_TYPE_TRANSFER).setTo(to).setAmount(amount).setNonce(nonce).setGasLimit(gasLimit).setGasPrice(gasPrice)
            .setSignature(ByteString.copyFrom(signature)))
        .build();
  }

  private static void assertAcceptedTransactionResponse(pt.ulisboa.depchain.shared.network.model.InboundPacket responsePacket, String message, List<StartedServer> servers) {
    assertResponseNotNull(responsePacket, message, servers);

    ClientResponse response = decodeClientResponse(responsePacket);
    assertTrue(response.hasTransaction(), message);
    assertTrue(response.getTransaction().getReceipt().getSuccess(), message);
  }

  private void assertPersistedTransferForReplicas(List<String> replicaIds, long expectedAmount, long expectedNonce, long expectedHeight) throws IOException {
    ConfigParser config = ConfigParser.load(configPath);
    for (String replicaId : replicaIds) {
      BlockStore store = BlockStore.forReplica(config, replicaId);
      BlockStore.BlockDocument genesis = store.load(0L).orElseThrow(() -> new AssertionError("Genesis block was not persisted for " + replicaId));
      BlockStore.BlockDocument latest = store.loadLatest().orElseThrow(() -> new AssertionError("Latest block was not persisted for " + replicaId));

      assertEquals(expectedHeight, latest.height(), "Unexpected latest block height for " + replicaId);
      assertEquals(genesis.blockHash(), latest.previousBlockHash(), "Transfer block should link to the persisted genesis block");
      assertTrue(!latest.transactions().isEmpty(), "Persisted block should contain the transfer transaction for " + replicaId);
      assertEquals("TRANSFER", latest.transactions().getFirst().type(), "Persisted block should store a transfer for " + replicaId);
      assertEquals("DepCoin", latest.transactions().getFirst().currency(), "Persisted transfer should keep the DepCoin currency marker for " + replicaId);
      assertEquals(RECIPIENT, latest.transactions().getFirst().to(), "Persisted transfer should target the expected recipient for " + replicaId);
      assertEquals(Long.toString(expectedAmount), latest.transactions().getFirst().amount(), "Persisted transfer should keep the expected amount for " + replicaId);
      assertEquals(expectedNonce, latest.transactions().getFirst().nonce(), "Persisted transfer should keep the expected nonce for " + replicaId);
      assertEquals(TRANSFER_GAS_LIMIT, latest.gasUsed(), "Persisted transfer should keep the observed gas usage for " + replicaId);
      assertEquals(BlockStore.computeBlockHash(latest.previousBlockHash(), latest.gasUsed(), latest.transactions()), latest
          .blockHash(), "Persisted block should derive its hash from the previous block hash plus the block transactions for " + replicaId);
      assertTrue(latest.state().containsKey(RECIPIENT), "Persisted state should include the recipient for " + replicaId);
    }
  }

  private void assertPersistedBatchForReplicas(List<String> replicaIds) {
    try {
      ConfigParser config = ConfigParser.load(configPath);
      List<String> expectedFromOrder = List.of(clientAddress(config, 101L), clientAddress(config, 102L), clientAddress(config, 100L));
      List<Long> expectedGasPrices = List.of(9L, 5L, 1L);
      List<String> expectedAmounts = List.of("11", "7", "3");

      for (String replicaId : replicaIds) {
        BlockStore store = BlockStore.forReplica(config, replicaId);
        BlockStore.BlockDocument genesis = store.load(0L).orElseThrow(() -> new AssertionError("Genesis block was not persisted for " + replicaId));
        BlockStore.BlockDocument latest = store.loadLatest().orElseThrow(() -> new AssertionError("Latest block was not persisted for " + replicaId));

        assertEquals(1L, latest.height(), "Batch should be persisted as a single block for " + replicaId);
        assertEquals(genesis.blockHash(), latest.previousBlockHash(), "Batch block should link to the persisted genesis block for " + replicaId);
        assertEquals(3, latest.transactions().size(), "Batch block should contain all three transactions for " + replicaId);
        assertEquals(TRANSFER_GAS_LIMIT * 3L, latest.gasUsed(), "Batch block should aggregate gas used across all transactions for " + replicaId);
        assertEquals(expectedFromOrder, latest.transactions().stream().map(transaction -> transaction.from())
            .toList(), "Transactions should be ordered by descending gas price for " + replicaId);
        assertEquals(expectedGasPrices, latest.transactions().stream().map(transaction -> transaction.gasPrice())
            .toList(), "Persisted gas price order should match mempool priority for " + replicaId);
        assertEquals(expectedAmounts, latest.transactions().stream().map(transaction -> transaction.amount())
            .toList(), "Persisted transaction amounts should match the submitted batch for " + replicaId);
        assertEquals("21", latest.state().get(RECIPIENT).balance(), "Recipient balance should include the full batch for " + replicaId);
        assertEquals(BlockStore.computeBlockHash(latest.previousBlockHash(), latest.gasUsed(), latest.transactions()), latest
            .blockHash(), "Persisted batch block hash should match its transaction array for " + replicaId);
      }
    } catch (Exception exception) {
      throw new IllegalStateException("Failed to assert persisted batch state", exception);
    }
  }

  private void assertPersistedEqualGasTieBreakForReplicas(List<String> replicaIds) {
    try {
      ConfigParser config = ConfigParser.load(configPath);
      List<String> expectedFromOrder = List.of(clientAddress(config, 100L), clientAddress(config, 101L), clientAddress(config, 102L));
      List<String> expectedAmounts = List.of("5", "7", "3");

      for (String replicaId : replicaIds) {
        BlockStore.BlockDocument latest = BlockStore.forReplica(config, replicaId).loadLatest()
            .orElseThrow(() -> new AssertionError("Latest block was not persisted for " + replicaId));

        assertEquals(1L, latest.height(), "Equal-gas batch should be persisted as a single block for " + replicaId);
        assertEquals(expectedFromOrder, latest.transactions().stream().map(transaction -> transaction.from())
            .toList(), "Transactions with equal gas price should use deterministic sender ordering for " + replicaId);
        assertEquals(expectedAmounts, latest.transactions().stream().map(transaction -> transaction.amount())
            .toList(), "Persisted equal-gas batch should keep the expected amounts for " + replicaId);
      }
    } catch (Exception exception) {
      throw new IllegalStateException("Failed to assert equal-gas tie-break state", exception);
    }
  }

  private void assertPersistedSplitBatchForReplicas(List<String> replicaIds) {
    try {
      ConfigParser config = ConfigParser.load(configPath);
      for (String replicaId : replicaIds) {
        BlockStore store = BlockStore.forReplica(config, replicaId);
        BlockStore.BlockDocument firstTransferBlock = store.load(1L).orElseThrow(() -> new AssertionError("First transfer block missing for " + replicaId));
        BlockStore.BlockDocument secondTransferBlock = store.load(2L).orElseThrow(() -> new AssertionError("Second transfer block missing for " + replicaId));

        assertEquals(1, firstTransferBlock.transactions().size(), "First block should contain exactly one oversized transaction for " + replicaId);
        assertEquals(1, secondTransferBlock.transactions().size(), "Second block should contain the deferred oversized transaction for " + replicaId);
        assertEquals(0L, firstTransferBlock.transactions().getFirst().nonce(), "First oversized block should keep the first nonce for " + replicaId);
        assertEquals(1L, secondTransferBlock.transactions().getFirst().nonce(), "Deferred oversized block should keep the second nonce for " + replicaId);
        assertEquals(secondTransferBlock.previousBlockHash(), firstTransferBlock.blockHash(), "Deferred block should link to the first oversized block for " + replicaId);
        assertEquals("6", secondTransferBlock.state().get(RECIPIENT).balance(), "Recipient balance should accumulate across split blocks for " + replicaId);
      }
    } catch (Exception exception) {
      throw new IllegalStateException("Failed to assert split batch persistence", exception);
    }
  }

  private void assertPersistedSameSenderRequestIdTieBreakForReplicas(List<String> replicaIds) {
    try {
      ConfigParser config = ConfigParser.load(configPath);
      List<String> expectedFromOrder = List.of(clientAddress(config, 100L), clientAddress(config, 100L), clientAddress(config, 101L));
      List<Long> expectedNonceOrder = List.of(0L, 1L, 0L);
      List<String> expectedAmounts = List.of("2", "4", "6");

      for (String replicaId : replicaIds) {
        BlockStore.BlockDocument latest = BlockStore.forReplica(config, replicaId).loadLatest()
            .orElseThrow(() -> new AssertionError("Latest block was not persisted for " + replicaId));

        assertEquals(expectedFromOrder, latest.transactions().stream().map(transaction -> transaction.from())
            .toList(), "Transactions should preserve deterministic same-sender ordering for " + replicaId);
        assertEquals(expectedNonceOrder, latest.transactions().stream().map(transaction -> transaction.nonce())
            .toList(), "Transactions should preserve nonce order implied by request-id ordering for " + replicaId);
        assertEquals(expectedAmounts, latest.transactions().stream().map(transaction -> transaction.amount())
            .toList(), "Persisted amounts should match the submitted same-sender tie-break batch for " + replicaId);
      }
    } catch (Exception exception) {
      throw new IllegalStateException("Failed to assert same-sender request-id tie-break state", exception);
    }
  }

  private static String clientAddress(ConfigParser config, long clientSenderId) {
    try {
      PublicKey clientPublicKey = PublicKeyLoader.loadClientPublicKey(config, clientSenderId);
      return CryptoUtil.deriveAddressHex(clientPublicKey);
    } catch (Exception exception) {
      throw new IllegalStateException("Could not derive client address for senderId " + clientSenderId, exception);
    }
  }

  private static BatchSubmission submitRequestsToLeaderWithoutWaiting(Path configPath, List<ClientRequest> requests) throws Exception {
    ConfigParser config = ConfigParser.load(configPath);
    Map<Long, PublicKey> staticPublicKeys = PublicKeyLoader.loadStaticPublicKeys(config);
    ConfigParser.ReplicaSection leader = config.requireReplicaById(LEADER_REPLICA_ID);
    InetSocketAddress leaderEndpoint = new InetSocketAddress(leader.host(), leader.clientPort());

    List<AuthenticatedLink> transports = new ArrayList<>(requests.size());
    try {
      for (ClientRequest request : requests) {
        long clientSenderId = request.getTransaction().getRequestKey().getClientSenderId();
        AuthenticatedLink transport = AuthenticatedLink.unbound(clientSenderId, PrivateKeyLoader.loadClientPrivateKey(config, clientSenderId), staticPublicKeys);
        transports.add(transport);
        transport.send(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE), ProtoValidationUtil.requireValid(request, "ClientRequest").toByteArray(), leaderEndpoint);
      }
      return new BatchSubmission(transports);
    } catch (Exception exception) {
      for (AuthenticatedLink transport : transports) {
        try {
          transport.close();
        } catch (Exception ignored) {
        }
      }
      throw exception;
    }
  }

  private static void waitForServersStartupWithDiagnostics(List<StartedServer> servers, Duration timeout) throws Exception {
    for (StartedServer server : servers) {
      if (!server.awaitReady(timeout)) {
        fail("Server did not become ready: " + System.lineSeparator() + server.describeState());
      }
    }
  }

  private record BatchSubmission(List<AuthenticatedLink> transports) implements AutoCloseable {
    @Override
    public void close() throws Exception {
      Exception firstFailure = null;
      for (AuthenticatedLink transport : transports) {
        try {
          transport.close();
        } catch (Exception exception) {
          if (firstFailure == null) {
            firstFailure = exception;
          }
        }
      }
      if (firstFailure != null) {
        throw firstFailure;
      }
    }
  }
}
