package pt.ulisboa.depchain.client;

import java.security.PrivateKey;
import java.util.concurrent.atomic.AtomicLong;

import com.google.protobuf.ByteString;

import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.QueryRequest;
import pt.ulisboa.depchain.proto.QueryType;
import pt.ulisboa.depchain.proto.TransactionRequest;
import pt.ulisboa.depchain.proto.TransactionType;
import pt.ulisboa.depchain.shared.utils.ClientRequestSignaturePayloadUtil;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;

final class SignedClientRequestFactory {
  private final long clientSenderId;
  private final PrivateKey clientPrivateKey;
  private final AtomicLong nextRequestId;

  SignedClientRequestFactory(long clientSenderId, PrivateKey clientPrivateKey) {
    this.clientSenderId = clientSenderId;
    this.clientPrivateKey = clientPrivateKey;
    this.nextRequestId = new AtomicLong(0L);
  }

  ClientRequest newTransactionRequest(TransactionType type, String recipientAddress, long amount, long nonce, long gasLimit, long gasPrice, byte[] input) throws Exception {
    long requestId = nextRequestId.getAndIncrement();
    byte[] signature = CryptoUtil.signEcdsa(ClientRequestSignaturePayloadUtil
        .signedTransactionRequestPayload(clientSenderId, requestId, type, recipientAddress, amount, nonce, gasLimit, gasPrice, input), clientPrivateKey);

    TransactionRequest.Builder transaction = TransactionRequest.newBuilder().setRequestKey(requestKey(requestId)).setType(type).setTo(recipientAddress).setAmount(amount)
        .setNonce(nonce).setGasLimit(gasLimit).setGasPrice(gasPrice).setSignature(ByteString.copyFrom(signature));
    if (input != null && input.length > 0) {
      transaction.setInput(ByteString.copyFrom(input));
    }
    return ClientRequest.newBuilder().setTransaction(transaction).build();
  }

  ClientRequest newQueryRequest(QueryType type, String ownerAddress) throws Exception {
    long requestId = nextRequestId.getAndIncrement();
    byte[] signature = CryptoUtil.signEcdsa(ClientRequestSignaturePayloadUtil.signedQueryRequestPayload(clientSenderId, requestId, type, ownerAddress), clientPrivateKey);

    QueryRequest query = QueryRequest.newBuilder().setRequestKey(requestKey(requestId)).setType(type).setOwner(ownerAddress).setSignature(ByteString.copyFrom(signature)).build();
    return ClientRequest.newBuilder().setQuery(query).build();
  }

  private ClientRequestKey requestKey(long requestId) {
    return ClientRequestKey.newBuilder().setClientSenderId(clientSenderId).setRequestId(requestId).build();
  }
}
