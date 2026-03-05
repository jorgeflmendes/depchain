package pt.ulisboa.depchain.client;

import java.nio.charset.StandardCharsets;

public final class ClientCodec {

  private ClientCodec() {}

  public static byte[] encodeRequest(ClientRequest req) {
    return req.value().getBytes(StandardCharsets.UTF_8);
  }

  public static ClientReply decodeReply(byte[] payload) {
    return new ClientReply(new String(payload, StandardCharsets.UTF_8));
  }
}