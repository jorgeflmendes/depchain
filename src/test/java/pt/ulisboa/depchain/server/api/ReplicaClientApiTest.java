package pt.ulisboa.depchain.server.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.KeyPair;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

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

class ReplicaClientApiTest {
  private static final String RECIPIENT = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

  @Test
  void validatesRequestsFromAnyConfiguredClientSenderId() throws Exception {
    KeyPair clientA = CryptoUtil.createEcKeyPair();
    KeyPair clientB = CryptoUtil.createEcKeyPair();
    ReplicaClientApi requestManager = new ReplicaClientApi(Map.of(100L, clientA.getPublic(), 101L, clientB.getPublic()), LoggerFactory.getLogger(ReplicaClientApiTest.class));

    ClientRequest requestFromClientB = signedTransferRequest(101L, 77L, 0L, clientB);

    assertTrue(requestManager.isValidClientRequest(requestFromClientB));
  }

  @Test
  void rejectsRequestsFromUnknownClientSenderIdEvenWithValidSignatureShape() throws Exception {
    KeyPair clientA = CryptoUtil.createEcKeyPair();
    KeyPair clientB = CryptoUtil.createEcKeyPair();
    ReplicaClientApi requestManager = new ReplicaClientApi(Map.of(100L, clientA.getPublic()), LoggerFactory.getLogger(ReplicaClientApiTest.class));

    ClientRequest requestFromUnknownClient = signedTransferRequest(101L, 88L, 0L, clientB);

    assertFalse(requestManager.isValidClientRequest(requestFromUnknownClient));
  }

  @Test
  void awaitNextPendingPrefersHigherGasPrice() throws Exception {
    KeyPair client = CryptoUtil.createEcKeyPair();
    ReplicaClientApi requestManager = new ReplicaClientApi(Map.of(100L, client.getPublic()), LoggerFactory.getLogger(ReplicaClientApiTest.class));

    ClientRequest lowFeeRequest = signedTransferRequest(100L, 1L, 0L, 1L, client);
    ClientRequest highFeeRequest = signedTransferRequest(100L, 2L, 1L, 5L, client);

    requestManager.registerClientRequest(lowFeeRequest, null, true);
    requestManager.registerClientRequest(highFeeRequest, null, true);

    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    assertEquals(highFeeRequest, requestManager.awaitNextPendingRequest(deadlineNanos));
    assertEquals(lowFeeRequest, requestManager.awaitNextPendingRequest(deadlineNanos));
  }

  @Test
  void awaitNextPendingPreservesArrivalOrderWhenGasPriceTies() throws Exception {
    KeyPair client = CryptoUtil.createEcKeyPair();
    ReplicaClientApi requestManager = new ReplicaClientApi(Map.of(100L, client.getPublic()), LoggerFactory.getLogger(ReplicaClientApiTest.class));

    ClientRequest firstRequest = signedTransferRequest(100L, 1L, 0L, 3L, client);
    ClientRequest secondRequest = signedTransferRequest(100L, 2L, 1L, 3L, client);

    requestManager.registerClientRequest(firstRequest, null, true);
    requestManager.registerClientRequest(secondRequest, null, true);

    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    assertEquals(firstRequest, requestManager.awaitNextPendingRequest(deadlineNanos));
    assertEquals(secondRequest, requestManager.awaitNextPendingRequest(deadlineNanos));
  }

  @Test
  void awaitNextPendingBatchReturnsGasPriceOrderedBatchWithinBlockGasLimit() throws Exception {
    KeyPair client = CryptoUtil.createEcKeyPair();
    ReplicaClientApi requestManager = new ReplicaClientApi(Map.of(100L, client.getPublic()), LoggerFactory.getLogger(ReplicaClientApiTest.class));

    ClientRequest lowFeeRequest = signedTransferRequest(100L, 1L, 0L, 50_000L, 1L, client);
    ClientRequest highestFeeRequest = signedTransferRequest(100L, 2L, 1L, 50_000L, 9L, client);
    ClientRequest mediumFeeRequest = signedTransferRequest(100L, 3L, 2L, 50_000L, 5L, client);

    requestManager.registerClientRequest(lowFeeRequest, null, true);
    requestManager.registerClientRequest(highestFeeRequest, null, true);
    requestManager.registerClientRequest(mediumFeeRequest, null, true);

    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    List<ClientRequest> batch = requestManager.awaitNextPendingBatch(deadlineNanos, 100_000L, 3);

    assertIterableEquals(List.of(highestFeeRequest, mediumFeeRequest), batch);
    assertEquals(lowFeeRequest, requestManager.awaitNextPendingRequest(deadlineNanos));
  }

  @Test
  void recordExecutedBatchMarksEveryRequestAsCompleted() throws Exception {
    KeyPair client = CryptoUtil.createEcKeyPair();
    ReplicaClientApi requestManager = new ReplicaClientApi(Map.of(100L, client.getPublic()), LoggerFactory.getLogger(ReplicaClientApiTest.class));

    ClientRequest firstRequest = signedTransferRequest(100L, 1L, 0L, 2L, client);
    ClientRequest secondRequest = signedTransferRequest(100L, 2L, 1L, 4L, client);

    requestManager.registerClientRequest(firstRequest, null, false);
    requestManager.registerClientRequest(secondRequest, null, false);
    requestManager.registerProposedCommand(nodeForBatch(firstRequest, secondRequest).getCommand());

    assertEquals(2, requestManager.recordExecutedNode(nodeForBatch(firstRequest, secondRequest)).size());
    assertEquals(0, requestManager.enqueuePendingRequestsIfLeader(true));
    assertNull(requestManager.awaitNextPendingRequest(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100)));
  }

  @Test
  void enqueuePendingKnownRequestsIfLeaderRequeuesByFeePriority() throws Exception {
    KeyPair client = CryptoUtil.createEcKeyPair();
    ReplicaClientApi requestManager = new ReplicaClientApi(Map.of(100L, client.getPublic()), LoggerFactory.getLogger(ReplicaClientApiTest.class));

    ClientRequest lowFeeRequest = signedTransferRequest(100L, 1L, 0L, 1L, client);
    ClientRequest highFeeRequest = signedTransferRequest(100L, 2L, 1L, 5L, client);

    requestManager.registerClientRequest(lowFeeRequest, null, false);
    requestManager.registerClientRequest(highFeeRequest, null, false);

    assertEquals(2, requestManager.enqueuePendingRequestsIfLeader(true));

    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    assertEquals(highFeeRequest, requestManager.awaitNextPendingRequest(deadlineNanos));
    assertEquals(lowFeeRequest, requestManager.awaitNextPendingRequest(deadlineNanos));
  }

  @Test
  void duplicateRequestKeyWithDifferentPayloadIsRejected() throws Exception {
    KeyPair client = CryptoUtil.createEcKeyPair();
    ReplicaClientApi requestManager = new ReplicaClientApi(Map.of(100L, client.getPublic()), LoggerFactory.getLogger(ReplicaClientApiTest.class));

    ClientRequest originalRequest = signedTransferRequest(100L, 9L, 0L, 1L, client);
    ClientRequest conflictingRequest = signedTransferRequest(100L, 9L, 0L, 9L, client);

    requestManager.registerClientRequest(originalRequest, null, true);
    requestManager.registerClientRequest(conflictingRequest, null, true);

    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    assertEquals(originalRequest, requestManager.awaitNextPendingRequest(deadlineNanos));
    assertEquals(1, requestManager.getPendingRequestCount());
  }

  @Test
  void rejectsTransactionsAboveBlockGasLimit() throws Exception {
    KeyPair client = CryptoUtil.createEcKeyPair();
    ReplicaClientApi requestManager = new ReplicaClientApi(Map.of(100L, client.getPublic()), LoggerFactory.getLogger(ReplicaClientApiTest.class));

    ClientRequest oversizedRequest = signedTransferRequest(100L, 9L, 0L, BlockStore.MAX_BLOCK_GAS_LIMIT + 1L, 1L, client);

    assertFalse(requestManager.isValidClientRequest(oversizedRequest));
  }

  @Test
  void completedRequestsAreNotReenqueuedOnLaterViewChanges() throws Exception {
    KeyPair client = CryptoUtil.createEcKeyPair();
    ReplicaClientApi requestManager = new ReplicaClientApi(Map.of(100L, client.getPublic()), LoggerFactory.getLogger(ReplicaClientApiTest.class));

    ClientRequest firstRequest = signedTransferRequest(100L, 1L, 0L, 1L, client);
    ClientRequest secondRequest = signedTransferRequest(100L, 2L, 1L, 5L, client);

    requestManager.registerClientRequest(firstRequest, null, false);
    requestManager.registerClientRequest(secondRequest, null, false);
    requestManager.recordExecutedNode(nodeForRequest(secondRequest));

    assertEquals(1, requestManager.enqueuePendingRequestsIfLeader(true));

    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    assertEquals(firstRequest, requestManager.awaitNextPendingRequest(deadlineNanos));
    assertNull(requestManager.awaitNextPendingRequest(deadlineNanos));
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
