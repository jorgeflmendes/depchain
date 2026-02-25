package pt.ulisboa.depchain.shared.udp.messages;

import java.io.Serial;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.Objects;
import java.util.UUID;

public record MessageRequest(UUID requestId, Object payload, InetAddress senderIp, int senderPort) implements Serializable {
  @Serial
  private static final long serialVersionUID = 2L;

  public MessageRequest {
    Objects.requireNonNull(requestId, "requestId cannot be null");
  }

  public MessageRequest(UUID requestId, Object payload) {
    this(requestId, payload, null, -1);
  }

  public MessageRequest withSender(InetAddress senderIp, int senderPort) {
    return new MessageRequest(requestId, payload, senderIp, senderPort);
  }
}
