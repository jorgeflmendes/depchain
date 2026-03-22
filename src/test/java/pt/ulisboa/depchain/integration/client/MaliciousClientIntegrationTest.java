package pt.ulisboa.depchain.integration.client;

import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import pt.ulisboa.depchain.integration.support.IntegrationHarness;
import pt.ulisboa.depchain.integration.support.IntegrationHarness.ManagedCluster;
import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.TransactionRequest;
import pt.ulisboa.depchain.proto.TransactionType;

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
}
