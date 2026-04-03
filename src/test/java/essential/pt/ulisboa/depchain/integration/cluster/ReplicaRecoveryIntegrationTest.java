package pt.ulisboa.depchain.integration.cluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.security.PrivateKey;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.validation.ProtoValidationUtil;

@Tag("integration")
class ReplicaRecoveryIntegrationTest extends IntegrationHarness {
  @Test
  void restartedFollowerCatchesUpAfterMissingMultipleBlocks() throws Exception {
    Path configPath = integrationConfigPath();
    cleanPersistedBlockData(configPath);
    populateConfig(configPath);

    List<StartedServer> servers = new ArrayList<>(startServers(REPLICA_IDS, configPath));
    StartedServer stoppedFollower = servers.stream().filter(server -> FOLLOWER_REPLICA_ID.equals(server.replicaId())).findFirst().orElseThrow();

    try {
      waitForServersStartup(servers, STARTUP_TIMEOUT);

      submitSuccessfulTransfer(configPath, 3L, 0L, STANDARD_REQUEST_TIMEOUT, servers);

      stopProcess(stoppedFollower);
      servers.remove(stoppedFollower);

      submitSuccessfulTransfer(configPath, 5L, 1L, STANDARD_REQUEST_TIMEOUT, servers);
      submitSuccessfulTransfer(configPath, 7L, 2L, STANDARD_REQUEST_TIMEOUT, servers);

      StartedServer restartedFollower = startServers(List.of(FOLLOWER_REPLICA_ID), configPath).getFirst();
      servers.add(restartedFollower);
      waitForServersStartup(List.of(restartedFollower), STARTUP_TIMEOUT);

      submitSuccessfulTransfer(configPath, 11L, 3L, VIEW_CHANGE_REQUEST_TIMEOUT, servers);
      awaitClusterHeight(configPath, 4L);
      submitSuccessfulTransfer(configPath, 13L, 4L, VIEW_CHANGE_REQUEST_TIMEOUT, servers);
      awaitClusterHeight(configPath, 5L);
      submitSuccessfulTransfer(configPath, 17L, 5L, VIEW_CHANGE_REQUEST_TIMEOUT, servers);
      awaitClusterHeight(configPath, 6L);

      try {
        await().forever().untilAsserted(() -> assertReplicaHeightsAndBalancesConverged(configPath));
      } catch (Throwable failure) {
        StringBuilder diagnostics = new StringBuilder("Replica recovery did not converge").append(System.lineSeparator());
        for (StartedServer server : servers) {
          diagnostics.append(server.describeState()).append(System.lineSeparator());
        }
        fail(diagnostics.toString(), failure);
      }
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

  private static void awaitClusterHeight(Path configPath, long expectedHeight) {
    await().forever().untilAsserted(() -> {
      ConfigParser config = ConfigParser.load(configPath);
      long leaderHeight = BlockStore.forReplica(config, LEADER_REPLICA_ID).loadLatest().orElseThrow().height();
      assertEquals(expectedHeight, leaderHeight, "Leader should advance to the next persisted height before the next recovery step");
    });
  }

  private static void submitSuccessfulTransfer(Path configPath, long amount, long nonce, Duration timeout, List<StartedServer> servers) throws Exception {
    InboundPacket responsePacket = broadcastClientRequestPayload(configPath, signedTransferRequest(configPath, amount, nonce).toByteArray(), timeout);
    assertResponseNotNull(responsePacket, "Cluster should return a coherent response for transfer amount=" + amount + " nonce=" + nonce, servers);
    ClientResponse response = decodeClientResponse(responsePacket);
    assertThat(response.hasTransaction()).isTrue();
    assertThat(response.getTransaction().hasReceipt()).isTrue();
    assertThat(response.getTransaction().getReceipt().getSuccess()).isTrue();
    rememberConsumedNonce(configPath, nonce);
  }

  private static ClientRequest signedTransferRequest(Path configPath, long amount, long nonce) throws Exception {
    ConfigParser config = ConfigParser.load(configPath);
    long clientSenderId = config.client().senderId();
    long requestId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    PrivateKey clientPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config);
    byte[] signature = CryptoUtil.signEcdsa(ClientRequestSignaturePayloadUtil
        .signedTransactionRequestPayload(clientSenderId, requestId, TransactionType.TRANSACTION_TYPE_TRANSFER, TEST_RECIPIENT_ADDRESS, amount, nonce, TEST_GAS_LIMIT, TEST_GAS_PRICE), clientPrivateKey);
    return ProtoValidationUtil.requireValid(ClientRequest.newBuilder()
        .setTransaction(TransactionRequest.newBuilder().setRequestKey(ClientRequestKey.newBuilder().setClientSenderId(clientSenderId).setRequestId(requestId))
            .setType(TransactionType.TRANSACTION_TYPE_TRANSFER).setTo(TEST_RECIPIENT_ADDRESS).setAmount(amount).setNonce(nonce).setGasLimit(TEST_GAS_LIMIT)
            .setGasPrice(TEST_GAS_PRICE).setSignature(ByteString.copyFrom(signature)))
        .build(), "ClientRequest");
  }

  private static void assertReplicaHeightsAndBalancesConverged(Path configPath) throws Exception {
    ConfigParser config = ConfigParser.load(configPath);
    Long expectedHeight = null;
    String expectedBalance = null;
    for (String replicaId : REPLICA_IDS) {
      BlockStore.BlockDocument latest = BlockStore.forReplica(config, replicaId).loadLatest().orElseThrow(() -> new AssertionError("Missing latest block for " + replicaId));
      if (expectedHeight == null) {
        expectedHeight = latest.height();
        expectedBalance = latest.state().get(TEST_RECIPIENT_ADDRESS).balance();
      } else {
        assertEquals(expectedHeight, latest.height(), "Replica " + replicaId + " should catch up to the cluster height");
        assertEquals(expectedBalance, latest.state().get(TEST_RECIPIENT_ADDRESS).balance(), "Replica " + replicaId + " should converge to the same state");
      }
    }
    assertEquals("56", expectedBalance, "Recovered follower should converge to the cumulative transferred balance");
  }
}
