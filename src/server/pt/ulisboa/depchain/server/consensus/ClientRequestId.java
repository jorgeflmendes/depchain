package pt.ulisboa.depchain.server.consensus;

import pt.ulisboa.depchain.shared.model.ClientRequest;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public record ClientRequestId(long clientSenderId, long requestId) {
  public ClientRequestId {
    ValidationUtils.requireNonNegativeLong(clientSenderId, "clientSenderId");
    ValidationUtils.requireNonNegativeLong(requestId, "requestId");
  }

  public static ClientRequestId from(ClientRequest request) {
    ValidationUtils.requireNonNull(request, "request");
    return new ClientRequestId(request.clientSenderId(), request.requestId());
  }
}
