package pt.ulisboa.depchain.integration.client;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.security.PrivateKey;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import com.google.protobuf.ByteString;

import pt.ulisboa.depchain.integration.support.IntegrationHarness;
import pt.ulisboa.depchain.integration.support.IntegrationHarness.ManagedCluster;
import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.ClientResponse;
import pt.ulisboa.depchain.proto.TransactionRequest;
import pt.ulisboa.depchain.proto.TransactionType;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.crypto.ClientRequestSignaturePayloadUtil;
import pt.ulisboa.depchain.shared.crypto.CryptoUtil;
import pt.ulisboa.depchain.shared.validation.ProtoValidationUtil;

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MaliciousClientIntegrationTest extends IntegrationHarness {
  private ManagedCluster sharedCluster;

  @BeforeAll
  void startSharedCluster() throws Exception {
    sharedCluster = startManagedCluster(REPLICA_IDS);
  }

  @AfterAll
  void stopSharedCluster() throws Exception {
    if (sharedCluster != null) {
      sharedCluster.close();
    }
  }

  @Test
  @Timeout(30)
  void clientCannotAuthenticateOnConsensusPortTest() throws Exception {
    ClientRequest request = signedTransferRequest(sharedCluster.clientSenderId(), 990L, 0L, sharedCluster.clientPrivateKey());
    byte[] payload = request.toByteArray();

    assertNull(sharedCluster.sendPayloadToConsensusPort(LEADER_REPLICA_ID, payload, Duration.ofSeconds(3)), "A client must not be able to authenticate on the consensus port");
    sharedCluster
        .assertRequestSucceeds("post-consensus-port-reject", STANDARD_REQUEST_TIMEOUT, "Cluster should remain responsive after rejecting client traffic on the consensus port");
  }

  @Test
  @Timeout(30)
  void clientRequestWithWrongSenderIdRejectedTest() throws Exception {
    ClientRequest forgedSenderRequest = signedTransferRequest(sharedCluster.clientSenderId() + 1, 991L, 0L, sharedCluster.clientPrivateKey());

    assertNull(sharedCluster
        .sendPayloadToClientPort(LEADER_REPLICA_ID, forgedSenderRequest.toByteArray(), Duration.ofSeconds(3)), "Requests with a mismatched client sender id must be rejected");
    sharedCluster.assertRequestSucceeds("post-wrong-sender", STANDARD_REQUEST_TIMEOUT, "Cluster should remain responsive after rejecting a forged client sender id");
  }

  @Test
  @Timeout(30)
  void malformedClientRequestPayloadRejectedTest() throws Exception {
    assertNull(sharedCluster.sendPayloadToClientPort(LEADER_REPLICA_ID, new byte[]{1, 2, 3, 4}, Duration.ofSeconds(3)), "Malformed protobuf client payloads must be rejected");
    sharedCluster.assertRequestSucceeds("post-malformed-payload", STANDARD_REQUEST_TIMEOUT, "Cluster should remain responsive after malformed client payloads");
  }

  @Test
  @Timeout(30)
  void protoInvalidClientRequestRejectedTest() throws Exception {
    ClientRequest invalidRequest = ClientRequest.newBuilder()
        .setTransaction(TransactionRequest.newBuilder().setRequestKey(ClientRequestKey.newBuilder().setClientSenderId(sharedCluster.clientSenderId()).setRequestId(992L))
            .setType(TransactionType.TRANSACTION_TYPE_TRANSFER).setTo(TEST_RECIPIENT_ADDRESS).setAmount(TEST_TRANSFER_AMOUNT).setNonce(0L).setGasLimit(TEST_GAS_LIMIT)
            .setGasPrice(TEST_GAS_PRICE))
        .build();

    assertNull(sharedCluster
        .sendPayloadToClientPort(LEADER_REPLICA_ID, invalidRequest.toByteArray(), Duration.ofSeconds(3)), "Proto-valid but protovalidate-invalid client requests must be rejected");
    sharedCluster.assertRequestSucceeds("post-invalid-client-proto", STANDARD_REQUEST_TIMEOUT, "Cluster should remain responsive after rejecting invalid client protos");
  }

  @Test
  @Timeout(30)
  void clientRequestWithUnexpectedNonceRejectedTest() throws Exception {
    try (ManagedCluster cluster = startManagedCluster(REPLICA_IDS)) {
      ClientRequest invalidNonceRequest = signedTransferRequest(cluster.configPath(), TEST_RECIPIENT_ADDRESS, TEST_TRANSFER_AMOUNT, 999L, TEST_GAS_LIMIT, TEST_GAS_PRICE);

      var responsePacket = broadcastClientRequestPayload(cluster.configPath(), ProtoValidationUtil.requireValid(invalidNonceRequest, "ClientRequest").toByteArray(), Duration
          .ofSeconds(3));
      assertFailedTransactionResponse(responsePacket, "invalid transaction nonce", "Requests with a nonce that does not match the sender account must fail");
      cluster.assertRequestSucceeds("post-invalid-nonce", STANDARD_REQUEST_TIMEOUT, "Cluster should remain responsive after rejecting an invalid nonce");
    }
  }

  @Test
  @Timeout(30)
  void clientRequestWithoutEnoughBalanceForAmountPlusGasRejectedTest() throws Exception {
    try (ManagedCluster cluster = startManagedCluster(REPLICA_IDS)) {
      ClientRequest insufficientFundsRequest = signedTransferRequest(cluster.configPath(), TEST_RECIPIENT_ADDRESS, 9_000_000_000L, 0L, TEST_GAS_LIMIT, TEST_GAS_PRICE);

      var responsePacket = broadcastClientRequestPayload(cluster.configPath(), ProtoValidationUtil.requireValid(insufficientFundsRequest, "ClientRequest").toByteArray(), Duration
          .ofSeconds(3));
      assertFailedTransactionResponse(responsePacket, "insufficient DepCoin balance", "Requests that cannot pay amount plus max gas must fail");
      cluster.assertRequestSucceeds("post-insufficient-balance", STANDARD_REQUEST_TIMEOUT, "Cluster should remain responsive after rejecting insufficient-balance requests");
    }
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
    assertTrue(response.hasTransaction(), message);
    assertTrue(response.getTransaction().hasReceipt(), message);
    assertFalse(response.getTransaction().getReceipt().getSuccess(), message);
    assertTrue(response.getTransaction().getReceipt().getErrorMessage().contains(expectedMessageSnippet), message);
  }
}
