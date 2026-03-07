package pt.ulisboa.depchain.client;

import pt.ulisboa.depchain.shared.utils.SerializationUtil;

public final class ClientCodec {

  private ClientCodec() {}

  public static byte[] encodeRequest(ClientRequest req) {
    return SerializationUtil.encodeString(req.value());
  }

  public static ClientReply decodeReply(byte[] payload) {
    return new ClientReply(SerializationUtil.decodeString(payload));
  }
}