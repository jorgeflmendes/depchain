package pt.ulisboa.depchain.server.consensus.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.KeyPair;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.Node;
import pt.ulisboa.depchain.proto.NodeCommand;
import pt.ulisboa.depchain.proto.TransactionNodeCommand;
import pt.ulisboa.depchain.proto.TransactionRequest;
import pt.ulisboa.depchain.proto.TransactionType;
import pt.ulisboa.depchain.shared.utils.ClientRequestSignaturePayloadUtil;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;

class ClientRequestManagerTest {
  private static final String RECIPIENT = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

  @Test
  void validatesRequestsFromAnyConfiguredClientSenderId() throws Exception {
    KeyPair clientA = CryptoUtil.newECKeyPair();
    KeyPair clientB = CryptoUtil.newECKeyPair();
    ClientRequestManager requestManager = new ClientRequestManager(Map.of(100L, clientA.getPublic(), 101L, clientB.getPublic()),
        LoggerFactory.getLogger(ClientRequestManagerTest.class));

    ClientRequest requestFromClientB = signedTransferRequest(101L, 77L, 0L, clientB);

    assertTrue(requestManager.hasValidClientRequest(requestFromClientB));
  }

  @Test
  void rejectsRequestsFromUnknownClientSenderIdEvenWithValidSignatureShape() throws Exception {
    KeyPair clientA = CryptoUtil.newECKeyPair();
    KeyPair clientB = CryptoUtil.newECKeyPair();
    ClientRequestManager requestManager = new ClientRequestManager(Map.of(100L, clientA.getPublic()), LoggerFactory.getLogger(ClientRequestManagerTest.class));

    ClientRequest requestFromUnknownClient = signedTransferRequest(101L, 88L, 0L, clientB);

    assertFalse(requestManager.hasValidClientRequest(requestFromUnknownClient));
  }

  @Test
  void awaitNextPendingPrefersHigherGasPrice() throws Exception {
    KeyPair client = CryptoUtil.newECKeyPair();
    ClientRequestManager requestManager = new ClientRequestManager(Map.of(100L, client.getPublic()), LoggerFactory.getLogger(ClientRequestManagerTest.class));

    ClientRequest lowFeeRequest = signedTransferRequest(100L, 1L, 0L, 1L, client);
    ClientRequest highFeeRequest = signedTransferRequest(100L, 2L, 1L, 5L, client);

    requestManager.onClientRequest(lowFeeRequest, null, true);
    requestManager.onClientRequest(highFeeRequest, null, true);

    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    assertEquals(highFeeRequest, requestManager.awaitNextPending(deadlineNanos));
    assertEquals(lowFeeRequest, requestManager.awaitNextPending(deadlineNanos));
  }

  @Test
  void awaitNextPendingPreservesArrivalOrderWhenGasPriceTies() throws Exception {
    KeyPair client = CryptoUtil.newECKeyPair();
    ClientRequestManager requestManager = new ClientRequestManager(Map.of(100L, client.getPublic()), LoggerFactory.getLogger(ClientRequestManagerTest.class));

    ClientRequest firstRequest = signedTransferRequest(100L, 1L, 0L, 3L, client);
    ClientRequest secondRequest = signedTransferRequest(100L, 2L, 1L, 3L, client);

    requestManager.onClientRequest(firstRequest, null, true);
    requestManager.onClientRequest(secondRequest, null, true);

    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    assertEquals(firstRequest, requestManager.awaitNextPending(deadlineNanos));
    assertEquals(secondRequest, requestManager.awaitNextPending(deadlineNanos));
  }

  @Test
  void enqueuePendingKnownRequestsIfLeaderRequeuesByFeePriority() throws Exception {
    KeyPair client = CryptoUtil.newECKeyPair();
    ClientRequestManager requestManager = new ClientRequestManager(Map.of(100L, client.getPublic()), LoggerFactory.getLogger(ClientRequestManagerTest.class));

    ClientRequest lowFeeRequest = signedTransferRequest(100L, 1L, 0L, 1L, client);
    ClientRequest highFeeRequest = signedTransferRequest(100L, 2L, 1L, 5L, client);

    requestManager.onClientRequest(lowFeeRequest, null, false);
    requestManager.onClientRequest(highFeeRequest, null, false);

    assertEquals(2, requestManager.enqueuePendingKnownRequestsIfLeader(true));

    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    assertEquals(highFeeRequest, requestManager.awaitNextPending(deadlineNanos));
    assertEquals(lowFeeRequest, requestManager.awaitNextPending(deadlineNanos));
  }

  @Test
  void duplicateRequestKeyWithDifferentPayloadIsRejected() throws Exception {
    KeyPair client = CryptoUtil.newECKeyPair();
    ClientRequestManager requestManager = new ClientRequestManager(Map.of(100L, client.getPublic()), LoggerFactory.getLogger(ClientRequestManagerTest.class));

    ClientRequest originalRequest = signedTransferRequest(100L, 9L, 0L, 1L, client);
    ClientRequest conflictingRequest = signedTransferRequest(100L, 9L, 0L, 9L, client);

    requestManager.onClientRequest(originalRequest, null, true);
    requestManager.onClientRequest(conflictingRequest, null, true);

    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    assertEquals(originalRequest, requestManager.awaitNextPending(deadlineNanos));
    assertEquals(1, requestManager.pendingCount());
  }

  @Test
  void completedRequestsAreNotReenqueuedOnLaterViewChanges() throws Exception {
    KeyPair client = CryptoUtil.newECKeyPair();
    ClientRequestManager requestManager = new ClientRequestManager(Map.of(100L, client.getPublic()), LoggerFactory.getLogger(ClientRequestManagerTest.class));

    ClientRequest firstRequest = signedTransferRequest(100L, 1L, 0L, 1L, client);
    ClientRequest secondRequest = signedTransferRequest(100L, 2L, 1L, 5L, client);

    requestManager.onClientRequest(firstRequest, null, false);
    requestManager.onClientRequest(secondRequest, null, false);
    requestManager.markExecuted(nodeForRequest(secondRequest));

    assertEquals(1, requestManager.enqueuePendingKnownRequestsIfLeader(true));

    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    assertEquals(firstRequest, requestManager.awaitNextPending(deadlineNanos));
    assertNull(requestManager.awaitNextPending(deadlineNanos));
  }

  private static ClientRequest signedTransferRequest(long clientSenderId, long requestId, long nonce, KeyPair keyPair) throws Exception {
    return signedTransferRequest(clientSenderId, requestId, nonce, 1L, keyPair);
  }

  private static ClientRequest signedTransferRequest(long clientSenderId, long requestId, long nonce, long gasPrice, KeyPair keyPair) throws Exception {
    byte[] signature = CryptoUtil.signEcdsa(ClientRequestSignaturePayloadUtil
        .signedTransactionRequestPayload(clientSenderId, requestId, TransactionType.TRANSACTION_TYPE_TRANSFER, RECIPIENT, 5L, nonce, 21_000L, gasPrice), keyPair.getPrivate());
    return ClientRequest.newBuilder()
        .setTransaction(TransactionRequest.newBuilder().setRequestKey(ClientRequestKey.newBuilder().setClientSenderId(clientSenderId).setRequestId(requestId))
            .setType(TransactionType.TRANSACTION_TYPE_TRANSFER).setTo(RECIPIENT).setAmount(5L).setNonce(nonce).setGasLimit(21_000L).setGasPrice(gasPrice)
            .setSignature(ByteString.copyFrom(signature)))
        .build();
  }

  private static Node nodeForRequest(ClientRequest request) {
    return Node.newBuilder().setNodeHash("node-hash-" + request.getTransaction().getRequestKey().getRequestId()).setParentNodeHash("hash-0").setViewNumber(1)
        .setCommand(NodeCommand.newBuilder().setTransaction(TransactionNodeCommand.newBuilder().setClientRequest(request))).build();
  }
}
