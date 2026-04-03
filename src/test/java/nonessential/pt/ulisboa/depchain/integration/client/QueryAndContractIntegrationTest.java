package pt.ulisboa.depchain.integration.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import org.hyperledger.besu.datatypes.Address;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;

import pt.ulisboa.depchain.client.api.ClientReplicaApi;
import pt.ulisboa.depchain.client.request.SignedClientRequestFactory;
import pt.ulisboa.depchain.integration.support.IntegrationHarness;
import pt.ulisboa.depchain.integration.support.IntegrationHarness.ManagedCluster;
import pt.ulisboa.depchain.integration.support.IntegrationHarness.StartedServer;
import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.ClientResponse;
import pt.ulisboa.depchain.proto.QueryResponse;
import pt.ulisboa.depchain.proto.QueryType;
import pt.ulisboa.depchain.proto.TransactionRequest;
import pt.ulisboa.depchain.proto.TransactionResponse;
import pt.ulisboa.depchain.proto.TransactionType;
import pt.ulisboa.depchain.server.execution.IstCoin;
import pt.ulisboa.depchain.server.node.BlockStore;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.crypto.ClientRequestSignaturePayloadUtil;
import pt.ulisboa.depchain.shared.crypto.CryptoUtil;
import pt.ulisboa.depchain.shared.crypto.key.PrivateKeyLoader;
import pt.ulisboa.depchain.shared.crypto.key.PublicKeyLoader;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.validation.ProtoValidationUtil;

@Tag("integration")
class QueryAndContractIntegrationTest extends IntegrationHarness {
  private static final String CLIENT_ID = "client";
  private static final String SPENDER_CLIENT_ID = "client2";
  private static final String RECIPIENT_ADDRESS = "cccccccccccccccccccccccccccccccccccccccc";
  private static final long INITIAL_ALLOWANCE = 60L;
  private static final long REAPPROVED_ALLOWANCE = 40L;

  @Test
  void queriesReflectDepCoinAndIstCoinTransfers() throws Exception {
    try (ManagedCluster cluster = startManagedCluster(REPLICA_IDS); ClientReplicaApi client = ClientReplicaApi.connect(cluster.configPath().toString(), CLIENT_ID)) {
      TransactionResponse depTransfer = client.transferDepCoin(RECIPIENT_ADDRESS, 7L, 0L, TEST_GAS_LIMIT, TEST_GAS_PRICE);
      TransactionResponse istTransfer = client.transferIstCoin(RECIPIENT_ADDRESS, 25L, 1L, 250_000L, TEST_GAS_PRICE);
      QueryResponse depBalance = client.getDepCoinBalance(RECIPIENT_ADDRESS);
      QueryResponse istBalance = client.getIstCoinBalance(RECIPIENT_ADDRESS);

      assertTrue(depTransfer.getReceipt().getSuccess(), "DepCoin transfer should succeed");
      assertTrue(istTransfer.getReceipt().getSuccess(), "IST Coin transfer should succeed");
      assertTrue(depBalance.getSuccess(), "DepCoin balance query should succeed");
      assertTrue(istBalance.getSuccess(), "IST Coin balance query should succeed");
      assertTrue(new BigInteger(1, depBalance.getReturnData().toByteArray()).compareTo(BigInteger.valueOf(7L)) >= 0, "Recipient should observe the transferred DepCoin");
      assertTrue(new BigInteger(1, istBalance.getReturnData().toByteArray()).compareTo(BigInteger.valueOf(25L)) >= 0, "Recipient should observe the transferred IST Coin");
    }
  }

  @Test
  void revertingContractCallReturnsFailedReceipt() throws Exception {
    try (ManagedCluster cluster = startManagedCluster(REPLICA_IDS); ClientReplicaApi client = ClientReplicaApi.connect(cluster.configPath().toString(), CLIENT_ID)) {
      String contractAddress = IstCoin.resolveContractAddress(cluster.configPath()).toHexString().substring(2);
      byte[] revertCallData = IstCoin.encodeTransferCallData(Address.ZERO, 1L).toArrayUnsafe();

      TransactionResponse response = client.callContract(contractAddress, 0L, 0L, 300_000L, TEST_GAS_PRICE, revertCallData);

      assertNotNull(response.getReceipt(), "Contract call should return a receipt");
      assertFalse(response.getReceipt().getSuccess(), "Contract call should fail when targeting the zero address");
      assertFalse(response.getReceipt().getErrorMessage().isBlank(), "Failed contract call should expose an error message");
    }
  }

  @Test
  void outOfGasContractCallReturnsFailedReceipt() throws Exception {
    try (ManagedCluster cluster = startManagedCluster(REPLICA_IDS); ClientReplicaApi client = ClientReplicaApi.connect(cluster.configPath().toString(), CLIENT_ID)) {
      String contractAddress = IstCoin.resolveContractAddress(cluster.configPath()).toHexString().substring(2);
      byte[] nameSelector = org.apache.tuweni.bytes.Bytes.fromHexString("0x06fdde03").toArrayUnsafe();

      TransactionResponse response = client.callContract(contractAddress, 0L, 0L, 1L, TEST_GAS_PRICE, nameSelector);

      assertNotNull(response.getReceipt(), "Out-of-gas call should return a receipt");
      assertFalse(response.getReceipt().getSuccess(), "Out-of-gas contract call should fail");
      assertTrue(response.getReceipt().getGasUsed() <= 1L, "Out-of-gas call must not report gas beyond the limit");
    }
  }

  @Test
  void queryStillSucceedsAfterLeaderCrash() throws Exception {
    try (ManagedCluster cluster = startManagedCluster(REPLICA_IDS); ClientReplicaApi client = ClientReplicaApi.connect(cluster.configPath().toString(), CLIENT_ID)) {
      cluster.assertRequestSucceeds("query-before-leader-crash", STANDARD_REQUEST_TIMEOUT, "Cluster should handle a write before leader crash");
      StartedServer crashedLeader = cluster.servers().getFirst();
      stopProcess(crashedLeader);

      QueryResponse depBalance = client.getDepCoinBalance(client.getWalletAddress());
      QueryResponse istBalance = client.getIstCoinBalance(client.getWalletAddress());

      assertTrue(depBalance.getSuccess(), "DepCoin balance query should still succeed after leader crash");
      assertTrue(istBalance.getSuccess(), "IST Coin balance query should still succeed after leader crash");
    }
  }

  @Test
  void queriesRemainAvailableWhileWritesAreBeingCommitted() throws Exception {
    try (ManagedCluster cluster = startManagedCluster(REPLICA_IDS)) {
      SignedClientRequestFactory requestFactory = new SignedClientRequestFactory(cluster.clientSenderId(), cluster.clientPrivateKey());
      String walletAddress = clientWalletAddress(cluster.configPath(), cluster.clientSenderId());
      CountDownLatch writesStarted = new CountDownLatch(1);
      CompletableFuture<Void> writer = CompletableFuture.runAsync(() -> {
        try {
          for (int i = 0; i < 4; i++) {
            writesStarted.countDown();
            TransactionResponse response = submitTransfer(cluster, requestFactory, i + 1L, i);
            assertTrue(response.getReceipt().getSuccess(), "Concurrent write should succeed");
          }
        } catch (Exception exception) {
          throw new RuntimeException(exception);
        }
      });

      writesStarted.await();
      for (int i = 0; i < 4; i++) {
        QueryResponse depBalance = queryReplica(cluster, FOLLOWER_REPLICA_ID, requestFactory, QueryType.QUERY_TYPE_DEPCOIN_BALANCE, walletAddress);
        QueryResponse istBalance = queryReplica(cluster, FOLLOWER_REPLICA_ID, requestFactory, QueryType.QUERY_TYPE_IST_COIN_BALANCE, walletAddress);
        assertTrue(depBalance.getSuccess(), "DepCoin query should succeed while writes are in flight");
        assertTrue(istBalance.getSuccess(), "IST query should succeed while writes are in flight");
      }

      writer.get();
      org.awaitility.Awaitility.await().forever().untilAsserted(() -> {
        QueryResponse recipientBalance = queryReplica(cluster, LEADER_REPLICA_ID, requestFactory, QueryType.QUERY_TYPE_DEPCOIN_BALANCE, RECIPIENT_ADDRESS);
        assertTrue(recipientBalance.getSuccess(), "Recipient query should succeed after concurrent writes");
        assertTrue(new BigInteger(1, recipientBalance.getReturnData().toByteArray())
            .compareTo(BigInteger.valueOf(10L)) >= 0, "Recipient should observe the cumulative transferred balance");
      });
    }
  }

  @Test
  void istTransferFailsForClientWithoutTokenBalance() throws Exception {
    try (ManagedCluster cluster = startManagedCluster(REPLICA_IDS); ClientReplicaApi spender = ClientReplicaApi.connect(cluster.configPath().toString(), SPENDER_CLIENT_ID)) {
      TransactionResponse response = spender.transferIstCoin(RECIPIENT_ADDRESS, 1L, 0L, 250_000L, TEST_GAS_PRICE);

      assertNotNull(response.getReceipt(), "Failed IST transfer should return a receipt");
      assertFalse(response.getReceipt().getSuccess(), "Client without IST balance should not be able to transfer tokens");
    }
  }

  @Test
  void approvalFrontRunningScenarioHoldsAtClusterLevel() throws Exception {
    try (ManagedCluster cluster = startManagedCluster(REPLICA_IDS)) {
      ConfigParser config = ConfigParser.load(cluster.configPath());
      Address contractAddress = IstCoin.resolveContractAddress(cluster.configPath());
      long ownerSenderId = config.requireClientById(CLIENT_ID).senderId();
      long spenderSenderId = config.requireClientById(SPENDER_CLIENT_ID).senderId();
      PrivateKey ownerPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config, ownerSenderId);
      PrivateKey spenderPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config, spenderSenderId);
      String ownerWalletAddress = clientWalletAddress(cluster.configPath(), ownerSenderId);
      String spenderWalletAddress = clientWalletAddress(cluster.configPath(), spenderSenderId);
      Address ownerAddress = Address.fromHexString("0x" + ownerWalletAddress);
      Address spenderAddress = Address.fromHexString("0x" + spenderWalletAddress);

      TransactionResponse initialApproval = submitContractCall(config, ownerSenderId, ownerPrivateKey, 1L, 0L, 300_000L, TEST_GAS_PRICE, contractAddress, IstCoin
          .encodeApproveCallData(spenderAddress, INITIAL_ALLOWANCE).toArrayUnsafe());
      assertNotNull(initialApproval.getReceipt(), "Initial approval should return a receipt");
      assertTrue(initialApproval.getReceipt().getSuccess(), "Initial approval should succeed");

      CountDownLatch contendersReady = new CountDownLatch(2);
      CountDownLatch startRace = new CountDownLatch(1);
      CompletableFuture<TransactionResponse> ownerReset = CompletableFuture.supplyAsync(() -> unchecked(() -> {
        contendersReady.countDown();
        startRace.await();
        return submitContractCall(config, ownerSenderId, ownerPrivateKey, 2L, 1L, 300_000L, TEST_GAS_PRICE, contractAddress, IstCoin.encodeApproveCallData(spenderAddress, 0L)
            .toArrayUnsafe());
      }));
      CompletableFuture<TransactionResponse> competingSpend = CompletableFuture.supplyAsync(() -> unchecked(() -> {
        contendersReady.countDown();
        startRace.await();
        return submitContractCall(config, spenderSenderId, spenderPrivateKey, 1L, 0L, 400_000L, 10L, contractAddress, IstCoin
            .encodeTransferFromCallData(ownerAddress, spenderAddress, INITIAL_ALLOWANCE).toArrayUnsafe());
      }));

      contendersReady.await();
      startRace.countDown();

      TransactionResponse resetResponse = ownerReset.get();
      TransactionResponse competingSpendResponse = competingSpend.get();
      assertNotNull(resetResponse.getReceipt(), "Reset request should return a receipt");
      assertNotNull(competingSpendResponse.getReceipt(), "Competing spend should return a receipt");
      assertTrue(resetResponse.getReceipt().getSuccess(), () -> "Zero-reset approval should succeed but failed with: " + resetResponse.getReceipt().getErrorMessage());
      assertTrue(competingSpendResponse.getReceipt()
          .getSuccess(), () -> "Front-running spend should succeed exactly once but failed with: " + competingSpendResponse.getReceipt().getErrorMessage());

      awaitIstBalanceAtReplica(config, LEADER_REPLICA_ID, ownerSenderId, ownerPrivateKey, spenderWalletAddress, BigInteger.valueOf(INITIAL_ALLOWANCE));

      TransactionResponse extraSpendAfterReset = submitContractCall(config, spenderSenderId, spenderPrivateKey, 2L, 1L, 400_000L, 10L, contractAddress, IstCoin
          .encodeTransferFromCallData(ownerAddress, spenderAddress, REAPPROVED_ALLOWANCE).toArrayUnsafe());
      TransactionResponse explicitReapproval = submitContractCall(config, ownerSenderId, ownerPrivateKey, 3L, 2L, 300_000L, TEST_GAS_PRICE, contractAddress, IstCoin
          .encodeApproveCallData(spenderAddress, REAPPROVED_ALLOWANCE).toArrayUnsafe());
      TransactionResponse approvedSpend = submitContractCall(config, spenderSenderId, spenderPrivateKey, 3L, 2L, 400_000L, 10L, contractAddress, IstCoin
          .encodeTransferFromCallData(ownerAddress, spenderAddress, REAPPROVED_ALLOWANCE).toArrayUnsafe());
      TransactionResponse excessSpend = submitContractCall(config, spenderSenderId, spenderPrivateKey, 4L, 3L, 400_000L, 10L, contractAddress, IstCoin
          .encodeTransferFromCallData(ownerAddress, spenderAddress, 1L).toArrayUnsafe());

      assertNotNull(extraSpendAfterReset.getReceipt(), "Spend after reset should return a receipt");
      assertNotNull(explicitReapproval.getReceipt(), "Explicit reapproval should return a receipt");
      assertNotNull(approvedSpend.getReceipt(), "Approved spend should return a receipt");
      assertNotNull(excessSpend.getReceipt(), "Excess spend should return a receipt");
      assertFalse(extraSpendAfterReset.getReceipt().getSuccess(), "Spend after zero reset should fail until the owner explicitly reapproves");
      assertTrue(explicitReapproval.getReceipt()
          .getSuccess(), () -> "Reapproval after zero reset should succeed but failed with: " + explicitReapproval.getReceipt().getErrorMessage());
      assertTrue(approvedSpend.getReceipt().getSuccess(), () -> "Approved post-reset spend should succeed but failed with: " + approvedSpend.getReceipt().getErrorMessage());
      assertFalse(excessSpend.getReceipt().getSuccess(), "Additional spend without allowance should fail");

      awaitIstBalanceAtReplica(config, LEADER_REPLICA_ID, ownerSenderId, ownerPrivateKey, spenderWalletAddress, BigInteger.valueOf(INITIAL_ALLOWANCE + REAPPROVED_ALLOWANCE));

      SignedClientRequestFactory ownerQueryFactory = new SignedClientRequestFactory(ownerSenderId, ownerPrivateKey);
      QueryResponse spenderBalance = queryReplica(config, LEADER_REPLICA_ID, ownerQueryFactory, ownerSenderId, ownerPrivateKey, QueryType.QUERY_TYPE_IST_COIN_BALANCE, spenderWalletAddress);
      assertTrue(spenderBalance.getSuccess(), "Final spender balance query should succeed");
      assertEquals(BigInteger.valueOf(INITIAL_ALLOWANCE + REAPPROVED_ALLOWANCE), decodeUnsigned(spenderBalance.getReturnData()
          .toByteArray()), "Cluster-level frontrunning scenario should leave the spender with exactly the authorised amount");
    }
  }

  @Test
  void nativeTransferToContractAccountFailsAtClusterLevel() throws Exception {
    try (ManagedCluster cluster = startManagedCluster(REPLICA_IDS); ClientReplicaApi client = ClientReplicaApi.connect(cluster.configPath().toString(), CLIENT_ID)) {
      String contractAddress = IstCoin.resolveContractAddress(cluster.configPath()).toHexString().substring(2);

      TransactionResponse response = client.transferDepCoin(contractAddress, 1L, 0L, TEST_GAS_LIMIT, TEST_GAS_PRICE);

      assertNotNull(response.getReceipt(), "Transfer to contract should still return a receipt");
      assertFalse(response.getReceipt().getSuccess(), "Native transfer to contract account must fail");
      assertTrue(response.getReceipt().getErrorMessage().contains("contract"), "Failure should explain that the target is a contract");
    }
  }

  @Test
  void contractCallToUnknownTargetFailsAtClusterLevel() throws Exception {
    try (ManagedCluster cluster = startManagedCluster(REPLICA_IDS); ClientReplicaApi client = ClientReplicaApi.connect(cluster.configPath().toString(), CLIENT_ID)) {
      String unknownContractAddress = "dddddddddddddddddddddddddddddddddddddddd";
      byte[] nameSelector = org.apache.tuweni.bytes.Bytes.fromHexString("0x06fdde03").toArrayUnsafe();

      TransactionResponse response = client.callContract(unknownContractAddress, 0L, 0L, 300_000L, TEST_GAS_PRICE, nameSelector);

      assertNotNull(response.getReceipt(), "Unknown-target contract call should return a receipt");
      assertFalse(response.getReceipt().getSuccess(), "Contract call to unknown target must fail");
      assertTrue(response.getReceipt().getErrorMessage().contains("unknown target"), "Failure should expose the missing target account");
    }
  }

  @Test
  void transferFromWithoutAllowanceFailsAtClusterLevel() throws Exception {
    try (ManagedCluster cluster = startManagedCluster(REPLICA_IDS);
        ClientReplicaApi owner = ClientReplicaApi.connect(cluster.configPath().toString(), CLIENT_ID);
        ClientReplicaApi spender = ClientReplicaApi.connect(cluster.configPath().toString(), SPENDER_CLIENT_ID)) {
      Address contractAddress = IstCoin.resolveContractAddress(cluster.configPath());
      Address ownerAddress = Address.fromHexString("0x" + owner.getWalletAddress());
      Address spenderAddress = Address.fromHexString("0x" + spender.getWalletAddress());

      TransactionResponse response = spender
          .callContract(contractAddress.toHexString().substring(2), 0L, 0L, 400_000L, 10L, IstCoin.encodeTransferFromCallData(ownerAddress, spenderAddress, 1L).toArrayUnsafe());

      assertNotNull(response.getReceipt(), "transferFrom without allowance should return a receipt");
      assertFalse(response.getReceipt().getSuccess(), "transferFrom without prior approval must fail");
    }
  }

  @Test
  void queryUnknownDepCoinAccountReturnsZero() throws Exception {
    try (ManagedCluster cluster = startManagedCluster(REPLICA_IDS); ClientReplicaApi client = ClientReplicaApi.connect(cluster.configPath().toString(), CLIENT_ID)) {
      QueryResponse depBalance = client.getDepCoinBalance("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

      assertTrue(depBalance.getSuccess(), "Unknown-account DepCoin query should still succeed");
      assertEquals(BigInteger.ZERO, decodeUnsigned(depBalance.getReturnData().toByteArray()), "Unknown account balance should be zero");
    }
  }

  @Test
  void istCoinStateRemainsAvailableAfterClusterRestart() throws Exception {
    Path configPath = integrationConfigPath();
    cleanPersistedBlockData(configPath);
    populateConfig(configPath);
    ConfigParser config = ConfigParser.load(configPath);
    long clientSenderId = config.client().senderId();
    PrivateKey clientPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config, clientSenderId);
    SignedClientRequestFactory requestFactory = new SignedClientRequestFactory(clientSenderId, clientPrivateKey);

    List<StartedServer> servers = startServers(HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS, configPath);
    try {
      waitForServersStartup(servers, STARTUP_TIMEOUT);
      String istContractAddress = IstCoin.resolveContractAddress(configPath).toHexString().substring(2);
      String expectedTransferCall = IstCoin.encodeTransferCallData(Address.fromHexString("0x" + RECIPIENT_ADDRESS), 30L).toHexString();
      TransactionResponse transfer = submitTransaction(config, clientSenderId, clientPrivateKey, requestFactory, TransactionType.TRANSACTION_TYPE_IST_COIN_TRANSFER, RECIPIENT_ADDRESS, 30L, 0L, 250_000L, TEST_GAS_PRICE, new byte[0]);
      assertNotNull(transfer.getReceipt(), "IST Coin transfer before restart should return a receipt");
      assertTrue(transfer.getReceipt().getSuccess(), "IST Coin transfer before restart should succeed");
      org.awaitility.Awaitility.await().forever().untilAsserted(() -> {
        for (String replicaId : HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS) {
          BlockStore.BlockDocument latest = BlockStore.forReplica(config, replicaId).loadLatest()
              .orElseThrow(() -> new AssertionError("Latest block was not persisted for " + replicaId));
          assertTrue(latest.height() >= 1L, "Transferred IST state should be persisted before shutdown for " + replicaId);
          assertTrue(!latest.transactions().isEmpty(), "Persisted block should contain the IST transfer before shutdown for " + replicaId);
          assertEquals("CONTRACT_CALL", latest.transactions().getFirst().type(), "Persisted block should store the IST transfer as a contract call before shutdown for "
              + replicaId);
          assertEquals(istContractAddress, latest.transactions().getFirst().to(), "Persisted IST transfer should target the IST contract before shutdown for " + replicaId);
          assertEquals("0", latest.transactions().getFirst().amount(), "Persisted IST transfer should use a zero native amount before shutdown for " + replicaId);
          assertEquals(expectedTransferCall, latest.transactions().getFirst().input(), "Persisted IST transfer should retain the encoded contract call before shutdown for "
              + replicaId);
        }
      });
    } finally {
      stopProcesses(servers);
    }

    List<StartedServer> restartedServers = startServers(HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS, configPath);
    try {
      waitForServersStartup(restartedServers, STARTUP_TIMEOUT);
      org.awaitility.Awaitility.await().forever().untilAsserted(() -> {
        QueryResponse istBalance = queryReplica(config, LEADER_REPLICA_ID, requestFactory, clientSenderId, clientPrivateKey, QueryType.QUERY_TYPE_IST_COIN_BALANCE, RECIPIENT_ADDRESS);
        assertTrue(istBalance.getSuccess(), "IST Coin balance query after restart should succeed");
        assertTrue(new BigInteger(1, istBalance.getReturnData().toByteArray())
            .compareTo(BigInteger.valueOf(30L)) >= 0, "Persisted IST Coin balance should survive cluster restart");
      });
    } finally {
      try {
        stopProcesses(restartedServers);
      } finally {
        cleanPersistedBlockData(configPath);
      }
    }
  }

  private static void awaitIstBalanceAtReplica(ConfigParser config, String replicaId, long clientSenderId, PrivateKey clientPrivateKey, String ownerAddress, BigInteger minimumBalance)
      throws Exception {
    SignedClientRequestFactory requestFactory = new SignedClientRequestFactory(clientSenderId, clientPrivateKey);
    org.awaitility.Awaitility.await().forever()
        .untilAsserted(() -> assertTrue(decodeUnsigned(queryReplica(config, replicaId, requestFactory, clientSenderId, clientPrivateKey, QueryType.QUERY_TYPE_IST_COIN_BALANCE, ownerAddress)
            .getReturnData().toByteArray()).compareTo(minimumBalance) >= 0));
  }

  private static TransactionResponse submitTransfer(ManagedCluster cluster, SignedClientRequestFactory requestFactory, long amount, long nonce) throws Exception {
    ClientRequest request = requestFactory
        .createTransactionRequest(TransactionType.TRANSACTION_TYPE_TRANSFER, RECIPIENT_ADDRESS, amount, nonce, TEST_GAS_LIMIT, TEST_GAS_PRICE, new byte[0]);
    byte[] payload = ProtoValidationUtil.requireValid(request, "ClientRequest").toByteArray();
    var responsePacket = broadcastClientRequestPayload(cluster.configPath(), payload, STANDARD_REQUEST_TIMEOUT);
    assertResponseNotNull(responsePacket, "Transfer should receive a response", cluster.servers());
    var response = decodeClientResponse(responsePacket);
    assertTrue(response.hasTransaction(), "Broadcast should return a transaction response");
    return response.getTransaction();
  }

  private static TransactionResponse submitTransaction(ConfigParser config, long clientSenderId, PrivateKey clientPrivateKey, SignedClientRequestFactory requestFactory, TransactionType type, String recipientAddress, long amount, long nonce, long gasLimit, long gasPrice, byte[] input)
      throws Exception {
    ClientRequest request = requestFactory.createTransactionRequest(type, recipientAddress, amount, nonce, gasLimit, gasPrice, input);
    ClientResponse response = submitClientPayloadDirectToLeaderAndAwaitResponse(config, clientSenderId, clientPrivateKey, ProtoValidationUtil.requireValid(request, "ClientRequest")
        .toByteArray());
    assertTrue(response.hasTransaction(), "Transaction submission should return a transaction response");
    return response.getTransaction();
  }

  private static TransactionResponse submitContractCall(ConfigParser config, long clientSenderId, PrivateKey clientPrivateKey, long requestId, long nonce, long gasLimit, long gasPrice, Address contractAddress, byte[] input)
      throws Exception {
    ClientResponse response = submitClientPayloadDirectToLeaderAndAwaitResponse(config, clientSenderId, clientPrivateKey, ProtoValidationUtil
        .requireValid(contractCallRequest(clientSenderId, requestId, nonce, gasLimit, gasPrice, contractAddress, input, clientPrivateKey), "ClientRequest").toByteArray());
    assertTrue(response.hasTransaction(), "Contract call should return a transaction response");
    return response.getTransaction();
  }

  private static QueryResponse queryReplica(ManagedCluster cluster, String replicaId, SignedClientRequestFactory requestFactory, QueryType queryType, String ownerAddress)
      throws Exception {
    return queryReplica(cluster.configPath(), replicaId, requestFactory, cluster.clientSenderId(), cluster.clientPrivateKey(), queryType, ownerAddress);
  }

  private static QueryResponse queryReplica(Path configPath, String replicaId, SignedClientRequestFactory requestFactory, long clientSenderId, PrivateKey clientPrivateKey, QueryType queryType, String ownerAddress)
      throws Exception {
    ConfigParser config = ConfigParser.load(configPath);
    return queryReplica(config, replicaId, requestFactory, clientSenderId, clientPrivateKey, queryType, ownerAddress);
  }

  private static QueryResponse queryReplica(ConfigParser config, String replicaId, SignedClientRequestFactory requestFactory, long clientSenderId, PrivateKey clientPrivateKey, QueryType queryType, String ownerAddress)
      throws Exception {
    ClientRequest queryRequest = requestFactory.createQueryRequest(queryType, ownerAddress);
    byte[] payload = ProtoValidationUtil.requireValid(queryRequest, "ClientRequest").toByteArray();
    var responsePacket = sendClientPayloadToReplicaAndAwaitResponse(config, replicaId, clientSenderId, clientPrivateKey, payload);
    assertNotNull(responsePacket, "Query should receive a response from " + replicaId);
    var response = decodeClientResponse(responsePacket);
    assertTrue(response.hasQuery(), "Replica should return a query response");
    return response.getQuery();
  }

  private static String clientWalletAddress(Path configPath, long clientSenderId) throws Exception {
    ConfigParser config = ConfigParser.load(configPath);
    return CryptoUtil.deriveAddressHex(PublicKeyLoader.loadClientPublicKey(config, clientSenderId));
  }

  private static ClientRequest contractCallRequest(long clientSenderId, long requestId, long nonce, long gasLimit, long gasPrice, Address contractAddress, byte[] input, PrivateKey clientPrivateKey)
      throws Exception {
    byte[] signaturePayload = ClientRequestSignaturePayloadUtil
        .signedTransactionRequestPayload(clientSenderId, requestId, TransactionType.TRANSACTION_TYPE_CONTRACT_CALL, contractAddress.toHexString()
            .substring(2), 0L, nonce, gasLimit, gasPrice, input);
    byte[] signature = CryptoUtil.signEcdsa(signaturePayload, clientPrivateKey);
    return ClientRequest.newBuilder()
        .setTransaction(TransactionRequest.newBuilder().setRequestKey(ClientRequestKey.newBuilder().setClientSenderId(clientSenderId).setRequestId(requestId))
            .setType(TransactionType.TRANSACTION_TYPE_CONTRACT_CALL).setTo(contractAddress.toHexString().substring(2)).setAmount(0L).setNonce(nonce).setGasLimit(gasLimit)
            .setGasPrice(gasPrice).setInput(ByteString.copyFrom(input)).setSignature(ByteString.copyFrom(signature)))
        .build();
  }

  private static ClientResponse submitClientPayloadDirectToLeaderAndAwaitResponse(ConfigParser config, long senderId, PrivateKey clientPrivateKey, byte[] payload)
      throws Exception {
    var responsePacket = sendClientPayloadToReplicaAndAwaitResponse(config, LEADER_REPLICA_ID, senderId, clientPrivateKey, payload);
    assertNotNull(responsePacket, "Direct leader submission should receive a response");
    return decodeClientResponse(responsePacket);
  }

  private static pt.ulisboa.depchain.shared.network.model.InboundPacket sendClientPayloadToReplicaAndAwaitResponse(ConfigParser config, String replicaId, long senderId, PrivateKey clientPrivateKey, byte[] payload)
      throws Exception {
    Map<Long, PublicKey> staticPublicKeys = PublicKeyLoader.loadStaticPublicKeys(config);
    ConfigParser.ReplicaSection replica = config.requireReplicaById(replicaId);
    InetSocketAddress endpoint = new InetSocketAddress(replica.host(), replica.clientPort());
    long connectionId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    try (AuthenticatedLink transport = AuthenticatedLink.unbound(senderId, clientPrivateKey, staticPublicKeys)) {
      transport.send(connectionId, payload, endpoint);
      var responseRef = new java.util.concurrent.atomic.AtomicReference<pt.ulisboa.depchain.shared.network.model.InboundPacket>();
      org.awaitility.Awaitility.await().forever().until(() -> {
        var inbound = transport.receive(receiveTimeoutMs(STANDARD_REQUEST_TIMEOUT));
        if (inbound != null && inbound.packet().getConnectionId() == connectionId) {
          responseRef.set(inbound);
          return true;
        }
        return false;
      });
      return responseRef.get();
    }
  }

  private static <T> T unchecked(ThrowingSupplier<T> supplier) {
    try {
      return supplier.get();
    } catch (RuntimeException exception) {
      throw exception;
    } catch (Exception exception) {
      throw new RuntimeException(exception);
    }
  }

  @FunctionalInterface
  private interface ThrowingSupplier<T> {
    T get() throws Exception;
  }

  private static BigInteger decodeUnsigned(byte[] encoded) {
    return new BigInteger(1, encoded);
  }
}
