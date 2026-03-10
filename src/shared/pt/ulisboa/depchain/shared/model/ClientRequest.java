package pt.ulisboa.depchain.shared.model;

import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Arrays;

import pt.ulisboa.depchain.shared.utils.CryptoUtil;
import pt.ulisboa.depchain.shared.utils.SerializationUtil;

public record ClientRequest(long clientSenderId, long requestId, String command, byte[] signature) {
  public ClientRequest {
    if (command == null) {
      throw new IllegalArgumentException("command cannot be null");
    }
    if (signature == null) {
      throw new IllegalArgumentException("signature cannot be null");
    }

    signature = Arrays.copyOf(signature, signature.length);
  }

  @Override
  public byte[] signature() {
    return Arrays.copyOf(signature, signature.length);
  }

  public static ClientRequest signed(long clientSenderId, long requestId, String command, PrivateKey privateKey) throws Exception {
    byte[] payload = SerializationUtil.encodeSignedClientRequestPayload(clientSenderId, requestId, command);
    byte[] signature = CryptoUtil.signEcdsa(payload, privateKey);
    return new ClientRequest(clientSenderId, requestId, command, signature);
  }

  public boolean hasValidSignature(PublicKey publicKey) throws Exception {
    byte[] payload = SerializationUtil.encodeSignedClientRequestPayload(clientSenderId, requestId, command);
    return CryptoUtil.verifyEcdsa(payload, signature, publicKey);
  }
}
