package pt.ulisboa.depchain.integration.cluster;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;

import pt.ulisboa.depchain.integration.support.IntegrationHarness;
import pt.ulisboa.depchain.integration.support.IntegrationHarness.StartedServer;
import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.ClientResponse;
import pt.ulisboa.depchain.proto.TransactionRequest;
import pt.ulisboa.depchain.proto.TransactionType;
import pt.ulisboa.depchain.server.runtime.BlockStore;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.keys.PrivateKeyLoader;
import pt.ulisboa.depchain.shared.utils.ClientRequestSignaturePayloadUtil;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;
import pt.ulisboa.depchain.shared.utils.ProtoValidationUtil;

@Tag("integration")
class BlockPersistenceIntegrationTest extends IntegrationHarness {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String RECIPIENT = "cccccccccccccccccccccccccccccccccccccccc";
  private static final long TRANSFER_AMOUNT = 7L;

  @Test
  @Timeout(60)
  void transactionExecutionPersistsBlockWithTransactionAndUpdatedState() throws Exception {
    cleanBlockDataDirectory();
    BlockStore.defaultStore().ensureGenesisPersisted();

    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    List<StartedServer> servers = startServers(HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS, configPath);
    try {
      waitForServersStartupWithDiagnostics(servers, Duration.ofSeconds(35));

      ClientRequest transfer = signedTransferRequest(configPath, RECIPIENT, TRANSFER_AMOUNT, 0L, 21_000L, 1L);
      byte[] payload = ProtoValidationUtil.requireValid(transfer, "ClientRequest").toByteArray();

      var responsePacket = broadcastClientRequestPayload(configPath, payload, STANDARD_REQUEST_TIMEOUT);
      assertResponseNotNull(responsePacket, "Transaction request should receive a response", servers);

      ClientResponse response = decodeClientResponse(responsePacket);
      assertTrue(response.hasTransaction());
      assertTrue(response.getTransaction().getAccepted());
      assertTrue(response.getTransaction().getReceipt().getSuccess());

      await().atMost(Duration.ofSeconds(12)).untilAsserted(() -> {
        BlockStore.BlockDocument block = findPersistedTransferBlock().orElseThrow(() -> new AssertionError("Transfer transaction block was not persisted"));
        BlockStore.BlockDocument genesis = findPersistedGenesisBlock().orElseThrow(() -> new AssertionError("Genesis block was not persisted"));

        assertEquals(RECIPIENT, block.transactions().getFirst().to());
        assertEquals(Long.toString(TRANSFER_AMOUNT), block.transactions().getFirst().amount());
        assertEquals(21_000L, block.gasUsed());
        assertEquals(genesis.blockHash(), block.previousBlockHash());
        assertTrue(block.state().containsKey(RECIPIENT));
        assertEquals(Long.toString(TRANSFER_AMOUNT), block.state().get(RECIPIENT).balance());
      });
    } finally {
      stopProcesses(servers);
      cleanBlockDataDirectory();
    }
  }

  private static ClientRequest signedTransferRequest(Path configPath, String to, long amount, long nonce, long gasLimit, long gasPrice) throws Exception {
    ConfigParser config = ConfigParser.load(configPath);
    long clientSenderId = config.client().senderId();
    long requestId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    PrivateKey clientPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config);

    byte[] signaturePayload = ClientRequestSignaturePayloadUtil
        .signedTransactionRequestPayload(clientSenderId, requestId, TransactionType.TRANSACTION_TYPE_TRANSFER, to, amount, nonce, gasLimit, gasPrice, null);
    byte[] signature = CryptoUtil.signEcdsa(signaturePayload, clientPrivateKey);

    return ClientRequest.newBuilder()
        .setTransaction(TransactionRequest.newBuilder().setRequestKey(ClientRequestKey.newBuilder().setClientSenderId(clientSenderId).setRequestId(requestId))
            .setType(TransactionType.TRANSACTION_TYPE_TRANSFER).setTo(to).setAmount(amount).setNonce(nonce).setGasLimit(gasLimit).setGasPrice(gasPrice)
            .setSignature(ByteString.copyFrom(signature)))
        .build();
  }

  private static Optional<BlockStore.BlockDocument> findPersistedTransferBlock() throws IOException {
    Path blocksDir = Path.of(System.getProperty("user.dir"), "data", "blocks");
    if (!Files.exists(blocksDir)) {
      return Optional.empty();
    }

    try (Stream<Path> paths = Files.list(blocksDir)) {
      return paths.filter(path -> path.getFileName().toString().startsWith("block-") && path.getFileName().toString().endsWith(".json")).sorted(Comparator.naturalOrder())
          .map(path -> {
            try {
              return JSON.readValue(path.toFile(), BlockStore.BlockDocument.class);
            } catch (IOException exception) {
              throw new IllegalStateException("Could not parse persisted block file " + path, exception);
            }
          }).filter(block -> !block.transactions().isEmpty() && "TRANSFER".equals(block.transactions().getFirst().type()) && RECIPIENT.equals(block.transactions().getFirst().to())
              && Long.toString(TRANSFER_AMOUNT).equals(block.transactions().getFirst().amount()))
          .findFirst();
    }
  }

  private static Optional<BlockStore.BlockDocument> findPersistedGenesisBlock() throws IOException {
    Path blocksDir = Path.of(System.getProperty("user.dir"), "data", "blocks");
    Path genesisPath = blocksDir.resolve("block-00000000.json");
    if (!Files.exists(genesisPath)) {
      return Optional.empty();
    }

    return Optional.of(JSON.readValue(genesisPath.toFile(), BlockStore.BlockDocument.class));
  }

  private static void cleanBlockDataDirectory() throws IOException {
    Path blocksDir = Path.of(System.getProperty("user.dir"), "data", "blocks");
    if (!Files.exists(blocksDir)) {
      return;
    }

    try (Stream<Path> paths = Files.walk(blocksDir)) {
      paths.sorted(Comparator.reverseOrder()).forEach(path -> {
        try {
          Files.deleteIfExists(path);
        } catch (IOException exception) {
          throw new IllegalStateException("Could not clean block path " + path, exception);
        }
      });
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
