package pt.ulisboa.depchain.server.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.security.KeyPair;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

import com.google.protobuf.ByteString;

import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.Node;
import pt.ulisboa.depchain.proto.NodeCommand;
import pt.ulisboa.depchain.proto.TransactionBatchNodeCommand;
import pt.ulisboa.depchain.proto.TransactionRequest;
import pt.ulisboa.depchain.proto.TransactionType;
import pt.ulisboa.depchain.server.node.BlockStore;
import pt.ulisboa.depchain.shared.crypto.ClientRequestSignaturePayloadUtil;
import pt.ulisboa.depchain.shared.crypto.CryptoUtil;

@ExtendWith(MockitoExtension.class)
@DisplayName("Replica client request ordering")
class ReplicaClientApiTest {
  private static final String RECIPIENT = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

  @Mock
  private Logger logger;

  @Test
  void validatesRequestsFromAnyConfiguredClientSenderId() throws Exception {
    KeyPair clientA = CryptoUtil.createEcKeyPair();
    KeyPair clientB = CryptoUtil.createEcKeyPair();
    ReplicaClientApi requestManager = new ReplicaClientApi(Map.of(100L, clientA.getPublic(), 101L, clientB.getPublic()), logger);

    ClientRequest requestFromClientB = signedTransferRequest(101L, 77L, 0L, clientB);

    assertThat(requestManager.isValidClientRequest(requestFromClientB)).isTrue();
  }

  @Test
  void rejectsRequestsFromUnknownClientSenderIdEvenWithValidSignatureShape() throws Exception {
    KeyPair clientA = CryptoUtil.createEcKeyPair();
    KeyPair clientB = CryptoUtil.createEcKeyPair();
    ReplicaClientApi requestManager = new ReplicaClientApi(Map.of(100L, clientA.getPublic()), logger);

    ClientRequest requestFromUnknownClient = signedTransferRequest(101L, 88L, 0L, clientB);

    assertThat(requestManager.isValidClientRequest(requestFromUnknownClient)).isFalse();
  }

  @Test
  void awaitNextPendingPrefersHigherGasPrice() throws Exception {
    KeyPair client = CryptoUtil.createEcKeyPair();
    ReplicaClientApi requestManager = new ReplicaClientApi(Map.of(100L, client.getPublic()), logger);

    ClientRequest lowFeeRequest = signedTransferRequest(100L, 1L, 0L, 1L, client);
    ClientRequest highFeeRequest = signedTransferRequest(100L, 2L, 1L, 5L, client);

    requestManager.registerClientRequest(lowFeeRequest, null, true);
    requestManager.registerClientRequest(highFeeRequest, null, true);

    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    assertThat(requestManager.awaitNextPendingRequest(deadlineNanos)).isEqualTo(highFeeRequest);
    assertThat(requestManager.awaitNextPendingRequest(deadlineNanos)).isEqualTo(lowFeeRequest);
  }

  @Test
  void awaitNextPendingBreaksGasPriceTiesByClientSenderId() throws Exception {
    KeyPair clientA = CryptoUtil.createEcKeyPair();
    KeyPair clientB = CryptoUtil.createEcKeyPair();
    ReplicaClientApi requestManager = new ReplicaClientApi(Map.of(100L, clientA.getPublic(), 101L, clientB.getPublic()), logger);

    ClientRequest higherSenderIdRequest = signedTransferRequest(101L, 10L, 0L, 3L, clientB);
    ClientRequest lowerSenderIdRequest = signedTransferRequest(100L, 20L, 0L, 3L, clientA);

    requestManager.registerClientRequest(higherSenderIdRequest, null, true);
    requestManager.registerClientRequest(lowerSenderIdRequest, null, true);

    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    assertThat(requestManager.awaitNextPendingRequest(deadlineNanos)).isEqualTo(lowerSenderIdRequest);
    assertThat(requestManager.awaitNextPendingRequest(deadlineNanos)).isEqualTo(higherSenderIdRequest);
  }

  @Test
  void awaitNextPendingBreaksRemainingGasPriceTiesByRequestIdBeforeNonce() throws Exception {
    KeyPair client = CryptoUtil.createEcKeyPair();
    ReplicaClientApi requestManager = new ReplicaClientApi(Map.of(100L, client.getPublic()), logger);

    ClientRequest lowerRequestIdHigherNonce = signedTransferRequest(100L, 1L, 9L, 3L, client);
    ClientRequest higherRequestIdLowerNonce = signedTransferRequest(100L, 2L, 0L, 3L, client);

    requestManager.registerClientRequest(higherRequestIdLowerNonce, null, true);
    requestManager.registerClientRequest(lowerRequestIdHigherNonce, null, true);

    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    assertThat(requestManager.awaitNextPendingRequest(deadlineNanos)).isEqualTo(lowerRequestIdHigherNonce);
    assertThat(requestManager.awaitNextPendingRequest(deadlineNanos)).isEqualTo(higherRequestIdLowerNonce);
  }

  @Test
  void awaitNextPendingBatchReturnsGasPriceOrderedBatchWithinBlockGasLimit() throws Exception {
    KeyPair client = CryptoUtil.createEcKeyPair();
    ReplicaClientApi requestManager = new ReplicaClientApi(Map.of(100L, client.getPublic()), logger);

    ClientRequest lowFeeRequest = signedTransferRequest(100L, 1L, 0L, 50_000L, 1L, client);
    ClientRequest highestFeeRequest = signedTransferRequest(100L, 2L, 1L, 50_000L, 9L, client);
    ClientRequest mediumFeeRequest = signedTransferRequest(100L, 3L, 2L, 50_000L, 5L, client);

    requestManager.registerClientRequest(lowFeeRequest, null, true);
    requestManager.registerClientRequest(highestFeeRequest, null, true);
    requestManager.registerClientRequest(mediumFeeRequest, null, true);

    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    List<ClientRequest> batch = requestManager.awaitNextPendingBatch(deadlineNanos, 100_000L, 3);

    assertThat(batch).containsExactly(highestFeeRequest, mediumFeeRequest);
    assertThat(requestManager.awaitNextPendingRequest(deadlineNanos)).isEqualTo(lowFeeRequest);
  }

  @Test
  void recordExecutedBatchMarksEveryRequestAsCompleted() throws Exception {
    KeyPair client = CryptoUtil.createEcKeyPair();
    ReplicaClientApi requestManager = new ReplicaClientApi(Map.of(100L, client.getPublic()), logger);

    ClientRequest firstRequest = signedTransferRequest(100L, 1L, 0L, 2L, client);
    ClientRequest secondRequest = signedTransferRequest(100L, 2L, 1L, 4L, client);

    requestManager.registerClientRequest(firstRequest, null, false);
    requestManager.registerClientRequest(secondRequest, null, false);
    requestManager.registerProposedCommand(nodeForBatch(firstRequest, secondRequest).getCommand());

    assertThat(requestManager.recordExecutedNode(nodeForBatch(firstRequest, secondRequest))).hasSize(2);
    assertThat(requestManager.enqueuePendingRequestsIfLeader(true)).isZero();
    assertThat(requestManager.awaitNextPendingRequest(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100))).isNull();
  }

  @Test
  void enqueuePendingKnownRequestsIfLeaderRequeuesByFeePriority() throws Exception {
    KeyPair client = CryptoUtil.createEcKeyPair();
    ReplicaClientApi requestManager = new ReplicaClientApi(Map.of(100L, client.getPublic()), logger);

    ClientRequest lowFeeRequest = signedTransferRequest(100L, 1L, 0L, 1L, client);
    ClientRequest highFeeRequest = signedTransferRequest(100L, 2L, 1L, 5L, client);

    requestManager.registerClientRequest(lowFeeRequest, null, false);
    requestManager.registerClientRequest(highFeeRequest, null, false);

    assertThat(requestManager.enqueuePendingRequestsIfLeader(true)).isEqualTo(2);

    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    assertThat(requestManager.awaitNextPendingRequest(deadlineNanos)).isEqualTo(highFeeRequest);
    assertThat(requestManager.awaitNextPendingRequest(deadlineNanos)).isEqualTo(lowFeeRequest);
  }

  @Test
  void duplicateRequestKeyWithDifferentPayloadIsRejected() throws Exception {
    KeyPair client = CryptoUtil.createEcKeyPair();
    ReplicaClientApi requestManager = new ReplicaClientApi(Map.of(100L, client.getPublic()), logger);

    ClientRequest originalRequest = signedTransferRequest(100L, 9L, 0L, 1L, client);
    ClientRequest conflictingRequest = signedTransferRequest(100L, 9L, 0L, 9L, client);

    requestManager.registerClientRequest(originalRequest, null, true);
    requestManager.registerClientRequest(conflictingRequest, null, true);

    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    assertThat(requestManager.awaitNextPendingRequest(deadlineNanos)).isEqualTo(originalRequest);
    assertThat(requestManager.getPendingRequestCount()).isEqualTo(1);
  }

  @Test
  void rejectsTransactionsAboveBlockGasLimit() throws Exception {
    KeyPair client = CryptoUtil.createEcKeyPair();
    ReplicaClientApi requestManager = new ReplicaClientApi(Map.of(100L, client.getPublic()), logger);

    ClientRequest oversizedRequest = signedTransferRequest(100L, 9L, 0L, BlockStore.MAX_BLOCK_GAS_LIMIT + 1L, 1L, client);

    assertThat(requestManager.isValidClientRequest(oversizedRequest)).isFalse();
  }

  @Test
  void completedRequestsAreNotReenqueuedOnLaterViewChanges() throws Exception {
    KeyPair client = CryptoUtil.createEcKeyPair();
    ReplicaClientApi requestManager = new ReplicaClientApi(Map.of(100L, client.getPublic()), logger);

    ClientRequest firstRequest = signedTransferRequest(100L, 1L, 0L, 1L, client);
    ClientRequest secondRequest = signedTransferRequest(100L, 2L, 1L, 5L, client);

    requestManager.registerClientRequest(firstRequest, null, false);
    requestManager.registerClientRequest(secondRequest, null, false);
    requestManager.recordExecutedNode(nodeForRequest(secondRequest));

    assertThat(requestManager.enqueuePendingRequestsIfLeader(true)).isEqualTo(1);

    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    assertThat(requestManager.awaitNextPendingRequest(deadlineNanos)).isEqualTo(firstRequest);
    assertThat(requestManager.awaitNextPendingRequest(deadlineNanos)).isNull();
  }

  private static ClientRequest signedTransferRequest(long clientSenderId, long requestId, long nonce, KeyPair keyPair) throws Exception {
    return signedTransferRequest(clientSenderId, requestId, nonce, 1L, keyPair);
  }

  private static ClientRequest signedTransferRequest(long clientSenderId, long requestId, long nonce, long gasPrice, KeyPair keyPair) throws Exception {
    return signedTransferRequest(clientSenderId, requestId, nonce, 21_000L, gasPrice, keyPair);
  }

  private static ClientRequest signedTransferRequest(long clientSenderId, long requestId, long nonce, long gasLimit, long gasPrice, KeyPair keyPair) throws Exception {
    byte[] signature = CryptoUtil.signEcdsa(ClientRequestSignaturePayloadUtil
        .signedTransactionRequestPayload(clientSenderId, requestId, TransactionType.TRANSACTION_TYPE_TRANSFER, RECIPIENT, 5L, nonce, gasLimit, gasPrice), keyPair.getPrivate());
    return ClientRequest.newBuilder()
        .setTransaction(TransactionRequest.newBuilder().setRequestKey(ClientRequestKey.newBuilder().setClientSenderId(clientSenderId).setRequestId(requestId))
            .setType(TransactionType.TRANSACTION_TYPE_TRANSFER).setTo(RECIPIENT).setAmount(5L).setNonce(nonce).setGasLimit(gasLimit).setGasPrice(gasPrice)
            .setSignature(ByteString.copyFrom(signature)))
        .build();
  }

  private static Node nodeForRequest(ClientRequest request) {
    return Node.newBuilder().setNodeHash("node-hash-" + request.getTransaction().getRequestKey().getRequestId()).setParentNodeHash("hash-0").setViewNumber(1)
        .setCommand(NodeCommand.newBuilder().setTransactionBatch(TransactionBatchNodeCommand.newBuilder().addClientRequests(request))).build();
  }

  private static Node nodeForBatch(ClientRequest... requests) {
    return Node.newBuilder().setNodeHash("node-hash-batch").setParentNodeHash("hash-0").setViewNumber(1)
        .setCommand(NodeCommand.newBuilder().setTransactionBatch(TransactionBatchNodeCommand.newBuilder().addAllClientRequests(List.of(requests)))).build();
  }
}
