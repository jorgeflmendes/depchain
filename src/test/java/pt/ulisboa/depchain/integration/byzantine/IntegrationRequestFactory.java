package pt.ulisboa.depchain.integration.byzantine;

import java.security.PrivateKey;

import com.google.protobuf.ByteString;

import pt.ulisboa.depchain.proto.AppendRequest;
import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.shared.utils.ClientRequestSignaturePayloadUtil;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;
import pt.ulisboa.depchain.shared.utils.ProtoValidationUtil;

final class IntegrationRequestFactory {
  private IntegrationRequestFactory() {
  }

  static ClientRequest signedAppendRequest(long clientSenderId, long requestId, String command, PrivateKey clientPrivateKey) throws Exception {
    byte[] signature = CryptoUtil.signEcdsa(ClientRequestSignaturePayloadUtil.signedAppendRequestPayload(clientSenderId, requestId, command), clientPrivateKey);
    return ProtoValidationUtil.requireValid(ClientRequest.newBuilder().setAppend(AppendRequest.newBuilder()
        .setRequestKey(ClientRequestKey.newBuilder().setClientSenderId(clientSenderId).setRequestId(requestId)).setValue(command).setSignature(ByteString.copyFrom(signature)))
        .build(), "ClientRequest");
  }
}
