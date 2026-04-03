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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import org.hyperledger.besu.datatypes.Address;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;

import pt.ulisboa.depchain.client.request.SignedClientRequestFactory;
import pt.ulisboa.depchain.integration.support.IntegrationHarness;
import pt.ulisboa.depchain.integration.support.IntegrationHarness.ManagedCluster;
import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.ClientResponse;
import pt.ulisboa.depchain.proto.QueryResponse;
import pt.ulisboa.depchain.proto.QueryType;
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
  private static final long INITIAL_ALLOWANCE = 60L;
  private static final long REAPPROVED_ALLOWANCE = 40L;

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

  private static void awaitIstBalanceAtReplica(ConfigParser config, String replicaId, long clientSenderId, PrivateKey clientPrivateKey, String ownerAddress, BigInteger minimumBalance)
      throws Exception {
    SignedClientRequestFactory requestFactory = new SignedClientRequestFactory(clientSenderId, clientPrivateKey);
    org.awaitility.Awaitility.await().forever()
        .untilAsserted(() -> assertTrue(decodeUnsigned(queryReplica(config, replicaId, requestFactory, clientSenderId, clientPrivateKey, QueryType.QUERY_TYPE_IST_COIN_BALANCE, ownerAddress)
            .getReturnData().toByteArray()).compareTo(minimumBalance) >= 0));
  }

  private static TransactionResponse submitContractCall(ConfigParser config, long clientSenderId, PrivateKey clientPrivateKey, long requestId, long nonce, long gasLimit, long gasPrice, Address contractAddress, byte[] input)
      throws Exception {
    ClientResponse response = submitClientPayloadDirectToLeaderAndAwaitResponse(config, clientSenderId, clientPrivateKey, ProtoValidationUtil
        .requireValid(contractCallRequest(clientSenderId, requestId, nonce, gasLimit, gasPrice, contractAddress, input, clientPrivateKey), "ClientRequest").toByteArray());
    assertTrue(response.hasTransaction(), "Contract call should return a transaction response");
    return response.getTransaction();
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
