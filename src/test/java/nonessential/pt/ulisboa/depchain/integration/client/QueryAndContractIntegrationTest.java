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
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import org.hyperledger.besu.datatypes.Address;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.google.protobuf.ByteString;

import pt.ulisboa.depchain.client.api.ClientReplicaApi;
import pt.ulisboa.depchain.integration.support.IntegrationHarness;
import pt.ulisboa.depchain.integration.support.IntegrationHarness.ManagedCluster;
import pt.ulisboa.depchain.integration.support.IntegrationHarness.StartedServer;
import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.QueryResponse;
import pt.ulisboa.depchain.proto.TransactionRequest;
import pt.ulisboa.depchain.proto.TransactionResponse;
import pt.ulisboa.depchain.proto.TransactionType;
import pt.ulisboa.depchain.server.execution.IstCoin;
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

  @Test
  @Timeout(60)
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
  @Timeout(60)
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
  @Timeout(60)
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
  @Timeout(60)
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
  @Timeout(90)
  void queriesRemainAvailableWhileWritesAreBeingCommitted() throws Exception {
    try (ManagedCluster cluster = startManagedCluster(REPLICA_IDS); ClientReplicaApi client = ClientReplicaApi.connect(cluster.configPath().toString(), CLIENT_ID)) {
      CompletableFuture<Void> writer = CompletableFuture.runAsync(() -> {
        try {
          for (int i = 0; i < 4; i++) {
            TransactionResponse response = client.transferDepCoin(RECIPIENT_ADDRESS, i + 1L, i, TEST_GAS_LIMIT, TEST_GAS_PRICE);
            assertTrue(response.getReceipt().getSuccess(), "Concurrent write should succeed");
          }
        } catch (Exception exception) {
          throw new RuntimeException(exception);
        }
      });

      for (int i = 0; i < 4; i++) {
        QueryResponse depBalance = client.getDepCoinBalance(client.getWalletAddress());
        QueryResponse istBalance = client.getIstCoinBalance(client.getWalletAddress());
        assertTrue(depBalance.getSuccess(), "DepCoin query should succeed while writes are in flight");
        assertTrue(istBalance.getSuccess(), "IST query should succeed while writes are in flight");
      }

      writer.get();
      QueryResponse recipientBalance = client.getDepCoinBalance(RECIPIENT_ADDRESS);
      assertTrue(recipientBalance.getSuccess(), "Recipient query should succeed after concurrent writes");
      assertTrue(new BigInteger(1, recipientBalance.getReturnData().toByteArray())
          .compareTo(BigInteger.valueOf(10L)) >= 0, "Recipient should observe the cumulative transferred balance");
    }
  }

  @Test
  @Timeout(60)
  void istTransferFailsForClientWithoutTokenBalance() throws Exception {
    try (ManagedCluster cluster = startManagedCluster(REPLICA_IDS); ClientReplicaApi spender = ClientReplicaApi.connect(cluster.configPath().toString(), SPENDER_CLIENT_ID)) {
      TransactionResponse response = spender.transferIstCoin(RECIPIENT_ADDRESS, 1L, 0L, 250_000L, TEST_GAS_PRICE);

      assertNotNull(response.getReceipt(), "Failed IST transfer should return a receipt");
      assertFalse(response.getReceipt().getSuccess(), "Client without IST balance should not be able to transfer tokens");
    }
  }

  @Test
  @Timeout(90)
  void approvalFrontRunningScenarioHoldsAtClusterLevel() throws Exception {
    try (ManagedCluster cluster = startManagedCluster(REPLICA_IDS);
        ClientReplicaApi owner = ClientReplicaApi.connect(cluster.configPath().toString(), CLIENT_ID);
        ClientReplicaApi spender = ClientReplicaApi.connect(cluster.configPath().toString(), SPENDER_CLIENT_ID)) {
      ConfigParser config = ConfigParser.load(cluster.configPath());
      Address contractAddress = IstCoin.resolveContractAddress(cluster.configPath());
      Address ownerAddress = Address.fromHexString("0x" + owner.getWalletAddress());
      Address spenderAddress = Address.fromHexString("0x" + spender.getWalletAddress());

      TransactionResponse initialApproval = owner
          .callContract(contractAddress.toHexString().substring(2), 0L, 0L, 300_000L, TEST_GAS_PRICE, IstCoin.encodeApproveCallData(spenderAddress, 100L).toArrayUnsafe());
      assertTrue(initialApproval.getReceipt().getSuccess(), "Initial approval should succeed");

      submitClientPayloadDirectToLeader(config, config.requireClientById(CLIENT_ID).senderId(), PrivateKeyLoader.loadClientPrivateKey(config), ProtoValidationUtil
          .requireValid(contractCallRequest(config.requireClientById(CLIENT_ID).senderId(), 1L, 1L, 300_000L, 1L, contractAddress, IstCoin.encodeApproveCallData(spenderAddress, 0L)
              .toArrayUnsafe(), PrivateKeyLoader.loadClientPrivateKey(config)), "ClientRequest")
          .toByteArray());
      submitClientPayloadDirectToLeader(config, config.requireClientById(SPENDER_CLIENT_ID).senderId(), PrivateKeyLoader
          .loadClientPrivateKey(config, config.requireClientById(SPENDER_CLIENT_ID).senderId()), ProtoValidationUtil
              .requireValid(contractCallRequest(config.requireClientById(SPENDER_CLIENT_ID).senderId(), 2L, 0L, 400_000L, 10L, contractAddress, IstCoin
                  .encodeTransferFromCallData(ownerAddress, spenderAddress, 100L)
                  .toArrayUnsafe(), PrivateKeyLoader.loadClientPrivateKey(config, config.requireClientById(SPENDER_CLIENT_ID).senderId())), "ClientRequest")
              .toByteArray());

      awaitClientObservedIstBalance(owner, spender.getWalletAddress(), BigInteger.valueOf(100L));

      TransactionResponse explicitReapproval = owner
          .callContract(contractAddress.toHexString().substring(2), 0L, 2L, 300_000L, TEST_GAS_PRICE, IstCoin.encodeApproveCallData(spenderAddress, 50L).toArrayUnsafe());
      TransactionResponse approvedSpend = spender
          .callContract(contractAddress.toHexString().substring(2), 0L, 1L, 400_000L, 10L, IstCoin.encodeTransferFromCallData(ownerAddress, spenderAddress, 50L).toArrayUnsafe());
      TransactionResponse excessSpend = spender
          .callContract(contractAddress.toHexString().substring(2), 0L, 2L, 400_000L, 10L, IstCoin.encodeTransferFromCallData(ownerAddress, spenderAddress, 1L).toArrayUnsafe());

      assertTrue(explicitReapproval.getReceipt().getSuccess(), "Reapproval after zero reset should succeed");
      assertTrue(approvedSpend.getReceipt().getSuccess(), "Approved post-reset spend should succeed");
      assertFalse(excessSpend.getReceipt().getSuccess(), "Additional spend without allowance should fail");
      assertEquals(BigInteger.valueOf(150L), decodeUnsigned(spender.getIstCoinBalance(spender.getWalletAddress()).getReturnData()
          .toByteArray()), "Cluster-level frontrunning scenario should leave the spender with exactly the authorised amount");
    }
  }

  @Test
  @Timeout(60)
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
  @Timeout(60)
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
  @Timeout(60)
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
  @Timeout(60)
  void queryUnknownDepCoinAccountReturnsZero() throws Exception {
    try (ManagedCluster cluster = startManagedCluster(REPLICA_IDS); ClientReplicaApi client = ClientReplicaApi.connect(cluster.configPath().toString(), CLIENT_ID)) {
      QueryResponse depBalance = client.getDepCoinBalance("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

      assertTrue(depBalance.getSuccess(), "Unknown-account DepCoin query should still succeed");
      assertEquals(BigInteger.ZERO, decodeUnsigned(depBalance.getReturnData().toByteArray()), "Unknown account balance should be zero");
    }
  }

  @Test
  @Timeout(90)
  void istCoinStateRemainsAvailableAfterClusterRestart() throws Exception {
    Path configPath = integrationConfigPath();
    cleanPersistedBlockData(configPath);
    populateConfig(configPath);

    List<StartedServer> servers = startServers(HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS, configPath);
    try {
      waitForServersStartup(servers, STARTUP_TIMEOUT);
      try (ClientReplicaApi client = ClientReplicaApi.connect(configPath.toString(), CLIENT_ID)) {
        TransactionResponse transfer = client.transferIstCoin(RECIPIENT_ADDRESS, 30L, 0L, 250_000L, TEST_GAS_PRICE);
        assertTrue(transfer.getReceipt().getSuccess(), "IST Coin transfer before restart should succeed");
      }
    } finally {
      stopProcesses(servers);
    }

    List<StartedServer> restartedServers = startServers(HONEST_WITH_ONE_BYZANTINE_REPLICA_IDS, configPath);
    try {
      waitForServersStartup(restartedServers, Duration.ofSeconds(35));
      try (ClientReplicaApi client = ClientReplicaApi.connect(configPath.toString(), CLIENT_ID)) {
        QueryResponse istBalance = client.getIstCoinBalance(RECIPIENT_ADDRESS);
        assertTrue(istBalance.getSuccess(), "IST Coin balance query after restart should succeed");
        assertTrue(new BigInteger(1, istBalance.getReturnData().toByteArray())
            .compareTo(BigInteger.valueOf(30L)) >= 0, "Persisted IST Coin balance should survive cluster restart");
      }
    } finally {
      try {
        stopProcesses(restartedServers);
      } finally {
        cleanPersistedBlockData(configPath);
      }
    }
  }

  private static void awaitClientObservedIstBalance(ClientReplicaApi ownerClient, String ownerAddress, BigInteger minimumBalance) throws Exception {
    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(15))
        .untilAsserted(() -> assertTrue(decodeUnsigned(ownerClient.getIstCoinBalance(ownerAddress).getReturnData().toByteArray()).compareTo(minimumBalance) >= 0));
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

  private static void submitClientPayloadDirectToLeader(ConfigParser config, long senderId, PrivateKey clientPrivateKey, byte[] payload) throws Exception {
    Map<Long, PublicKey> staticPublicKeys = PublicKeyLoader.loadStaticPublicKeys(config);
    ConfigParser.ReplicaSection leader = config.requireReplicaById(LEADER_REPLICA_ID);
    InetSocketAddress leaderEndpoint = new InetSocketAddress(leader.host(), leader.clientPort());
    try (AuthenticatedLink transport = AuthenticatedLink.unbound(senderId, clientPrivateKey, staticPublicKeys)) {
      transport.send(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE), payload, leaderEndpoint);
    }
  }

  private static BigInteger decodeUnsigned(byte[] encoded) {
    return new BigInteger(1, encoded);
  }
}
