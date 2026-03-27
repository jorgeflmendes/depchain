package pt.ulisboa.depchain.integration.cluster;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.time.Duration;
import java.util.List;
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
import pt.ulisboa.depchain.shared.validation.ProtoValidationUtil;

@Tag("integration")
class BlockPersistenceIntegrationTest extends IntegrationHarness {
  private static final long TRANSFER_GAS_LIMIT = 21_000L;
  private static final long TRANSFER_GAS_PRICE = 1L;
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
      cleanPersistedBlockData(configPath);
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

  private static ClientRequest signedTransferRequest(Path configPath, String to, long amount, long nonce, long gasLimit, long gasPrice) throws Exception {
    ConfigParser config = ConfigParser.load(configPath);
    long clientSenderId = config.client().senderId();
    long requestId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    PrivateKey clientPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config);

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

  private static void waitForServersStartupWithDiagnostics(List<StartedServer> servers, Duration timeout) throws Exception {
    for (StartedServer server : servers) {
      if (!server.awaitReady(timeout)) {
        fail("Server did not become ready: " + System.lineSeparator() + server.describeState());
      }
    }
  }
}
