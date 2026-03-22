package pt.ulisboa.depchain.shared.utils;

import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.QueryRequest;
import pt.ulisboa.depchain.proto.QueryType;
import pt.ulisboa.depchain.proto.TransactionRequest;
import pt.ulisboa.depchain.proto.TransactionType;

public final class ClientRequestSignaturePayloadUtil {
  private ClientRequestSignaturePayloadUtil() {
  }

  public static byte[] signedTransactionRequestPayload(long clientSenderId, long requestId, TransactionType type, String to, long amount, long nonce, long gasLimit, long gasPrice) {
    ValidationUtils.requireNonNegativeLong(clientSenderId, "clientSenderId");
    ValidationUtils.requireNonNegativeLong(requestId, "requestId");
    ValidationUtils.requireNonNull(type, "type");
    ValidationUtils.requireNonBlank(to, "to");
    ValidationUtils.requireNonNegativeLong(amount, "amount");
    ValidationUtils.requireNonNegativeLong(nonce, "nonce");
    ValidationUtils.requirePositiveLong(gasLimit, "gasLimit");
    ValidationUtils.requirePositiveLong(gasPrice, "gasPrice");

    TransactionRequest.Builder request = TransactionRequest.newBuilder().setRequestKey(ClientRequestKey.newBuilder().setClientSenderId(clientSenderId).setRequestId(requestId))
        .setType(type).setTo(to).setAmount(amount).setNonce(nonce).setGasLimit(gasLimit).setGasPrice(gasPrice);
    return request.build().toByteArray();
  }

  public static byte[] signedQueryRequestPayload(long clientSenderId, long requestId, QueryType type, String owner) {
    ValidationUtils.requireNonNegativeLong(clientSenderId, "clientSenderId");
    ValidationUtils.requireNonNegativeLong(requestId, "requestId");
    ValidationUtils.requireNonNull(type, "type");
    ValidationUtils.requireNonBlank(owner, "owner");

    QueryRequest.Builder request = QueryRequest.newBuilder().setRequestKey(ClientRequestKey.newBuilder().setClientSenderId(clientSenderId).setRequestId(requestId)).setType(type)
        .setOwner(owner);
    return request.build().toByteArray();
  }
}
