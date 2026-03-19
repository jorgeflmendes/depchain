package pt.ulisboa.depchain.shared.utils;

import pt.ulisboa.depchain.proto.AppendRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;

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
}
