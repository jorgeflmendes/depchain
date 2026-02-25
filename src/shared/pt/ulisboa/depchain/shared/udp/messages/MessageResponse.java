package pt.ulisboa.depchain.shared.udp.messages;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

public record MessageResponse(UUID requestId, boolean success, Object payload) implements Serializable {
  @Serial
  private static final long serialVersionUID = 2L;

  public MessageResponse {
    Objects.requireNonNull(requestId, "requestId cannot be null");
  }
}
