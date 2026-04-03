package pt.ulisboa.depchain.integration.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;

import pt.ulisboa.depchain.integration.byzantine.ByzantineAttackMode;
import pt.ulisboa.depchain.integration.support.ClusterIntegrationTestBase;
import pt.ulisboa.depchain.integration.support.IntegrationHarness.ManagedCluster;
import pt.ulisboa.depchain.integration.support.IntegrationHarness.StartedServer;
import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.ClientResponse;
import pt.ulisboa.depchain.proto.QueryRequest;
import pt.ulisboa.depchain.proto.QueryType;
import pt.ulisboa.depchain.proto.TransactionRequest;
import pt.ulisboa.depchain.proto.TransactionType;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.crypto.ClientRequestSignaturePayloadUtil;
import pt.ulisboa.depchain.shared.crypto.CryptoUtil;
import pt.ulisboa.depchain.shared.validation.ProtoValidationUtil;

@Tag("integration")
@DisplayName("Malicious client handling")
class MaliciousClientIntegrationTest extends ClusterIntegrationTestBase {
  private Path manualScenarioConfigPath;

  @BeforeAll
  void prepareManualScenarioConfig() throws Exception {
    manualScenarioConfigPath = integrationConfigPath();
    cleanPersistedBlockData(manualScenarioConfigPath);
    populateConfig(manualScenarioConfigPath);
  }

  @AfterAll
  void cleanupManualScenarioConfig() throws Exception {
    if (manualScenarioConfigPath != null) {
      try {
        cleanPersistedBlockData(manualScenarioConfigPath);
      } finally {
        cleanupIsolatedConfigDirectory(manualScenarioConfigPath);
      }
    }
  }

  @Test
  void clientCannotAuthenticateOnConsensusPortTest() throws Exception {
    ClientRequest request = signedTransferRequest(cluster().clientSenderId(), 990L, 0L, cluster().clientPrivateKey());
    byte[] payload = request.toByteArray();

    cluster().assertNoResponseFromConsensusPort(LEADER_REPLICA_ID, payload);
    cluster()
        .assertRequestSucceeds("post-consensus-port-reject", STANDARD_REQUEST_TIMEOUT, "Cluster should remain responsive after rejecting client traffic on the consensus port");
  }

  @Test
  void clientRequestWithWrongSenderIdRejectedTest() throws Exception {
    ClientRequest forgedSenderRequest = signedTransferRequest(cluster().clientSenderId() + 1, 991L, 0L, cluster().clientPrivateKey());

    cluster().assertNoResponseFromClientPort(LEADER_REPLICA_ID, forgedSenderRequest.toByteArray());
    cluster().assertRequestSucceeds("post-wrong-sender", STANDARD_REQUEST_TIMEOUT, "Cluster should remain responsive after rejecting a forged client sender id");
  }

  @Test
  void malformedClientRequestPayloadRejectedTest() throws Exception {
    cluster().assertNoResponseFromClientPort(LEADER_REPLICA_ID, new byte[]{1, 2, 3, 4});
    cluster().assertRequestSucceeds("post-malformed-payload", STANDARD_REQUEST_TIMEOUT, "Cluster should remain responsive after malformed client payloads");
  }

  @Test
  void protoInvalidClientRequestRejectedTest() throws Exception {
    ClientRequest invalidRequest = ClientRequest.newBuilder()
        .setTransaction(TransactionRequest.newBuilder().setRequestKey(ClientRequestKey.newBuilder().setClientSenderId(cluster().clientSenderId()).setRequestId(992L))
            .setType(TransactionType.TRANSACTION_TYPE_TRANSFER).setTo(TEST_RECIPIENT_ADDRESS).setAmount(TEST_TRANSFER_AMOUNT).setNonce(0L).setGasLimit(TEST_GAS_LIMIT)
            .setGasPrice(TEST_GAS_PRICE))
        .build();

    cluster().assertNoResponseFromClientPort(LEADER_REPLICA_ID, invalidRequest.toByteArray());
    cluster().assertRequestSucceeds("post-invalid-client-proto", STANDARD_REQUEST_TIMEOUT, "Cluster should remain responsive after rejecting invalid client protos");
  }

  @Test
  void clientRequestWithUnexpectedNonceRejectedTest() throws Exception {
    try (ManagedCluster cluster = startManagedCluster(REPLICA_IDS)) {
      ClientRequest invalidNonceRequest = signedTransferRequest(cluster.configPath(), TEST_RECIPIENT_ADDRESS, TEST_TRANSFER_AMOUNT, 999L, TEST_GAS_LIMIT, TEST_GAS_PRICE);

      var responsePacket = broadcastClientRequestPayload(cluster.configPath(), ProtoValidationUtil.requireValid(invalidNonceRequest, "ClientRequest")
          .toByteArray(), STANDARD_REQUEST_TIMEOUT);
      assertFailedTransactionResponse(responsePacket, "invalid transaction nonce", "Requests with a nonce that does not match the sender account must fail");
      cluster.assertRequestSucceeds("post-invalid-nonce", STANDARD_REQUEST_TIMEOUT, "Cluster should remain responsive after rejecting an invalid nonce");
    }
  }

  @Test
  void clientRequestWithoutEnoughBalanceForAmountPlusGasRejectedTest() throws Exception {
    try (ManagedCluster cluster = startManagedCluster(REPLICA_IDS)) {
      ClientRequest insufficientFundsRequest = signedTransferRequest(cluster.configPath(), TEST_RECIPIENT_ADDRESS, 9_000_000_000L, 0L, TEST_GAS_LIMIT, TEST_GAS_PRICE);

      var responsePacket = broadcastClientRequestPayload(cluster.configPath(), ProtoValidationUtil.requireValid(insufficientFundsRequest, "ClientRequest")
          .toByteArray(), STANDARD_REQUEST_TIMEOUT);
      assertFailedTransactionResponse(responsePacket, "insufficient DepCoin balance", "Requests that cannot pay amount plus max gas must fail");
      cluster.assertRequestSucceeds("post-insufficient-balance", STANDARD_REQUEST_TIMEOUT, "Cluster should remain responsive after rejecting insufficient-balance requests");
    }
  }

  @Test
  void colludingByzantineReplicaCannotForgeClientSuccessWithoutHonestReplyQuorum() throws Exception {
    Path configPath = manualScenarioConfigPath;
    cleanPersistedBlockData(configPath);

    List<StartedServer> servers = new ArrayList<>(startServers(HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS, configPath));
    StartedServer byzantineReplica = startByzantineServer(BYZANTINE_REPLICA_ID, configPath, ByzantineAttackMode.FORGED_CLIENT_SUCCESS_RESPONSE);
    servers.add(byzantineReplica);
    try {
      waitForServersStartup(servers, STARTUP_TIMEOUT);

      ClientRequest insufficientFundsRequest = signedTransferRequest(configPath, TEST_RECIPIENT_ADDRESS, 9_000_000_000L, 0L, TEST_GAS_LIMIT, TEST_GAS_PRICE);
      var responsePacket = broadcastClientRequestPayload(configPath, ProtoValidationUtil.requireValid(insufficientFundsRequest, "ClientRequest")
          .toByteArray(), VIEW_CHANGE_REQUEST_TIMEOUT);

      assertFailedTransactionResponse(responsePacket, "insufficient DepCoin balance", "One forged Byzantine success response must not outweigh the coherent honest failure quorum");
      assertByzantineAttackObserved(byzantineReplica, ByzantineAttackMode.FORGED_CLIENT_SUCCESS_RESPONSE, "Byzantine client-response forgery was never exercised");
      assertRequestSucceeds(configPath, "post-colluding-byzantine-client-response", STANDARD_REQUEST_TIMEOUT, servers, "Cluster should remain responsive after a Byzantine replica forges a client success response");
    } finally {
      try {
        stopProcesses(servers);
      } finally {
        cleanPersistedBlockData(configPath);
      }
    }
  }

  @Test
  void colludingByzantineLeaderCannotForgeClientSuccessWithoutHonestReplyQuorum() throws Exception {
    Path configPath = manualScenarioConfigPath;
    cleanPersistedBlockData(configPath);

    List<StartedServer> servers = new ArrayList<>(startServers(HONEST_WITH_BYZANTINE_LEADER_REPLICA_IDS, configPath));
    StartedServer byzantineLeader = startByzantineServer(LEADER_REPLICA_ID, configPath, ByzantineAttackMode.FORGED_CLIENT_SUCCESS_RESPONSE);
    servers.add(byzantineLeader);
    try {
      waitForServersStartup(servers, STARTUP_TIMEOUT);

      ClientRequest insufficientFundsRequest = signedTransferRequest(configPath, TEST_RECIPIENT_ADDRESS, 9_000_000_000L, 0L, TEST_GAS_LIMIT, TEST_GAS_PRICE);
      var responsePacket = broadcastClientRequestPayload(configPath, ProtoValidationUtil.requireValid(insufficientFundsRequest, "ClientRequest")
          .toByteArray(), VIEW_CHANGE_REQUEST_TIMEOUT);

      assertFailedTransactionResponse(responsePacket, "insufficient DepCoin balance", "A forged success reply from the Byzantine leader must not outweigh the coherent honest failure quorum");
      assertByzantineAttackObserved(byzantineLeader, ByzantineAttackMode.FORGED_CLIENT_SUCCESS_RESPONSE, "Byzantine leader client-response forgery was never exercised");
      assertRequestSucceeds(configPath, "post-colluding-byzantine-leader-response", STANDARD_REQUEST_TIMEOUT, servers, "Cluster should remain responsive after a Byzantine leader forges a client success response");
    } finally {
      try {
        stopProcesses(servers);
      } finally {
        cleanPersistedBlockData(configPath);
      }
    }
  }

  @Test
  void colludingByzantineReplicaCannotForceClientFailureWithoutHonestFailureQuorum() throws Exception {
    Path configPath = manualScenarioConfigPath;
    cleanPersistedBlockData(configPath);

    List<StartedServer> servers = new ArrayList<>(startServers(HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS, configPath));
    StartedServer byzantineReplica = startByzantineServer(BYZANTINE_REPLICA_ID, configPath, ByzantineAttackMode.FORGED_CLIENT_FAILURE_RESPONSE);
    servers.add(byzantineReplica);
    try {
      waitForServersStartup(servers, STARTUP_TIMEOUT);

      ClientRequest validRequest = signedTransferRequest(configPath, TEST_RECIPIENT_ADDRESS, TEST_TRANSFER_AMOUNT, 0L, TEST_GAS_LIMIT, TEST_GAS_PRICE);
      var responsePacket = broadcastClientRequestPayload(configPath, ProtoValidationUtil.requireValid(validRequest, "ClientRequest").toByteArray(), VIEW_CHANGE_REQUEST_TIMEOUT);

      assertResponseNotNull(responsePacket, "A single Byzantine forged failure must not block a coherent honest success quorum", servers);
      ClientResponse response = decodeClientResponse(responsePacket);
      assertThat(response.hasTransaction()).isTrue();
      assertThat(response.getTransaction().hasReceipt()).isTrue();
      assertThat(response.getTransaction().getReceipt().getSuccess()).isTrue();
      rememberConsumedNonce(configPath, 0L);
      assertByzantineAttackObserved(byzantineReplica, ByzantineAttackMode.FORGED_CLIENT_FAILURE_RESPONSE, "Byzantine client-failure forgery was never exercised");
      assertRequestSucceeds(configPath, "post-colluding-byzantine-failure", STANDARD_REQUEST_TIMEOUT, servers, "Cluster should remain responsive after a Byzantine replica forges a client failure response");
    } finally {
      try {
        stopProcesses(servers);
      } finally {
        cleanPersistedBlockData(configPath);
      }
    }
  }

  @Test
  void colludingByzantineLeaderCannotForceClientFailureWithoutHonestFailureQuorum() throws Exception {
    Path configPath = manualScenarioConfigPath;
    cleanPersistedBlockData(configPath);

    List<StartedServer> servers = new ArrayList<>(startServers(HONEST_WITH_BYZANTINE_LEADER_REPLICA_IDS, configPath));
    StartedServer byzantineLeader = startByzantineServer(LEADER_REPLICA_ID, configPath, ByzantineAttackMode.FORGED_CLIENT_FAILURE_RESPONSE);
    servers.add(byzantineLeader);
    try {
      waitForServersStartup(servers, STARTUP_TIMEOUT);

      ClientRequest validRequest = signedTransferRequest(configPath, TEST_RECIPIENT_ADDRESS, TEST_TRANSFER_AMOUNT, 0L, TEST_GAS_LIMIT, TEST_GAS_PRICE);
      var responsePacket = broadcastClientRequestPayload(configPath, ProtoValidationUtil.requireValid(validRequest, "ClientRequest").toByteArray(), VIEW_CHANGE_REQUEST_TIMEOUT);

      assertResponseNotNull(responsePacket, "A Byzantine leader forged failure must not block a coherent honest success quorum", servers);
      ClientResponse response = decodeClientResponse(responsePacket);
      assertThat(response.hasTransaction()).isTrue();
      assertThat(response.getTransaction().hasReceipt()).isTrue();
      assertThat(response.getTransaction().getReceipt().getSuccess()).isTrue();
      rememberConsumedNonce(configPath, 0L);
      assertByzantineAttackObserved(byzantineLeader, ByzantineAttackMode.FORGED_CLIENT_FAILURE_RESPONSE, "Byzantine leader client-failure forgery was never exercised");
      assertRequestSucceeds(configPath, "post-colluding-byzantine-leader-failure", STANDARD_REQUEST_TIMEOUT, servers, "Cluster should remain responsive after a Byzantine leader forges a client failure response");
    } finally {
      try {
        stopProcesses(servers);
      } finally {
        cleanPersistedBlockData(configPath);
      }
    }
  }

  @Test
  void malformedQueryOwnerRejectedTest() throws Exception {
    ConfigParser config = ConfigParser.load(cluster().configPath());
    long clientSenderId = config.client().senderId();
    PrivateKey clientPrivateKey = pt.ulisboa.depchain.shared.crypto.key.PrivateKeyLoader.loadClientPrivateKey(config);
    long requestId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    String malformedOwner = "not-a-valid-address";
    byte[] signature = CryptoUtil
        .signEcdsa(ClientRequestSignaturePayloadUtil.signedQueryRequestPayload(clientSenderId, requestId, QueryType.QUERY_TYPE_DEPCOIN_BALANCE, malformedOwner), clientPrivateKey);
    ClientRequest invalidQuery = ClientRequest.newBuilder()
        .setQuery(QueryRequest.newBuilder().setRequestKey(ClientRequestKey.newBuilder().setClientSenderId(clientSenderId).setRequestId(requestId))
            .setType(QueryType.QUERY_TYPE_DEPCOIN_BALANCE).setOwner(malformedOwner).setSignature(ByteString.copyFrom(signature)))
        .build();

    cluster().assertNoResponseFromClientPort(LEADER_REPLICA_ID, invalidQuery.toByteArray());
    cluster().assertRequestSucceeds("post-malformed-query-owner", STANDARD_REQUEST_TIMEOUT, "Cluster should remain responsive after rejecting malformed queries");
  }

  @Test
  void queryWithWrongSenderIdRejectedTest() throws Exception {
    ConfigParser config = ConfigParser.load(cluster().configPath());
    PrivateKey clientPrivateKey = pt.ulisboa.depchain.shared.crypto.key.PrivateKeyLoader.loadClientPrivateKey(config);
    long forgedSenderId = cluster().clientSenderId() + 1L;
    long requestId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    String owner = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    byte[] signature = CryptoUtil
        .signEcdsa(ClientRequestSignaturePayloadUtil.signedQueryRequestPayload(forgedSenderId, requestId, QueryType.QUERY_TYPE_DEPCOIN_BALANCE, owner), clientPrivateKey);
    ClientRequest forgedQuery = ClientRequest.newBuilder()
        .setQuery(QueryRequest.newBuilder().setRequestKey(ClientRequestKey.newBuilder().setClientSenderId(forgedSenderId).setRequestId(requestId))
            .setType(QueryType.QUERY_TYPE_DEPCOIN_BALANCE).setOwner(owner).setSignature(ByteString.copyFrom(signature)))
        .build();

    cluster().assertNoResponseFromClientPort(LEADER_REPLICA_ID, forgedQuery.toByteArray());
    cluster().assertRequestSucceeds("post-wrong-query-sender", STANDARD_REQUEST_TIMEOUT, "Cluster should remain responsive after rejecting forged query sender ids");
  }

  private static ClientRequest signedTransferRequest(Path configPath, String to, long amount, long nonce, long gasLimit, long gasPrice) throws Exception {
    ConfigParser config = ConfigParser.load(configPath);
    long clientSenderId = config.client().senderId();
    long requestId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    PrivateKey clientPrivateKey = pt.ulisboa.depchain.shared.crypto.key.PrivateKeyLoader.loadClientPrivateKey(config);
    byte[] signaturePayload = ClientRequestSignaturePayloadUtil
        .signedTransactionRequestPayload(clientSenderId, requestId, TransactionType.TRANSACTION_TYPE_TRANSFER, to, amount, nonce, gasLimit, gasPrice);
    byte[] signature = CryptoUtil.signEcdsa(signaturePayload, clientPrivateKey);
    return ClientRequest.newBuilder()
        .setTransaction(TransactionRequest.newBuilder().setRequestKey(ClientRequestKey.newBuilder().setClientSenderId(clientSenderId).setRequestId(requestId))
            .setType(TransactionType.TRANSACTION_TYPE_TRANSFER).setTo(to).setAmount(amount).setNonce(nonce).setGasLimit(gasLimit).setGasPrice(gasPrice)
            .setSignature(ByteString.copyFrom(signature)))
        .build();
  }

  private static void assertFailedTransactionResponse(pt.ulisboa.depchain.shared.network.model.InboundPacket responsePacket, String expectedMessageSnippet, String message) {
    assertResponseNotNull(responsePacket, message, List.of());
    ClientResponse response = decodeClientResponse(responsePacket);
    assertThat(response.hasTransaction()).as(message).isTrue();
    assertThat(response.getTransaction().hasReceipt()).as(message).isTrue();
    assertThat(response.getTransaction().getReceipt().getSuccess()).as(message).isFalse();
    assertThat(response.getTransaction().getReceipt().getErrorMessage()).contains(expectedMessageSnippet);
  }
}
