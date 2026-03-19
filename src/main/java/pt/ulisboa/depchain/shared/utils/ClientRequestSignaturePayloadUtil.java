package pt.ulisboa.depchain.shared.utils;

import pt.ulisboa.depchain.proto.AppendRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.TransactionRequest;
import pt.ulisboa.depchain.proto.TransactionType;

public final class ClientRequestSignaturePayloadUtil {
  private ClientRequestSignaturePayloadUtil() {
  }

  public static byte[] signedAppendRequestPayload(long clientSenderId, long requestId, String command) {
    ValidationUtils.requireNonNegativeLong(clientSenderId, "clientSenderId");
    ValidationUtils.requireNonNegativeLong(requestId, "requestId");
    ValidationUtils.requireNonNull(command, "command");

    return AppendRequest.newBuilder().setRequestKey(ClientRequestKey.newBuilder().setClientSenderId(clientSenderId).setRequestId(requestId)).setValue(command).build()
        .toByteArray();
  }

  public static byte[] signedTransactionRequestPayload(long clientSenderId, long requestId, TransactionType type, String to, long amount, long nonce, long gasLimit, long gasPrice, byte[] data) {
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
    if (data != null && data.length > 0) {
      request.setData(com.google.protobuf.ByteString.copyFrom(data));
    }
    return request.build().toByteArray();
  }
}
