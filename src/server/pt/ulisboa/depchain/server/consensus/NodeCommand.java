package pt.ulisboa.depchain.server.consensus;

import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public record NodeCommand(String value, ClientRequestId clientRequestId) {
  public static final String NO_OP_VALUE = "no-op";
  public static final String GENESIS_VALUE = "GENESIS";
  public static final NodeCommand NO_OP = new NodeCommand(NO_OP_VALUE, null);
  public static final NodeCommand GENESIS = new NodeCommand(GENESIS_VALUE, null);

  public NodeCommand {
    ValidationUtils.requireNonNull(value, "value");
  }

  public static NodeCommand clientRequest(String value, ClientRequestId clientRequestId) {
    ValidationUtils.requireNonNull(clientRequestId, "clientRequestId");
    return new NodeCommand(value, clientRequestId);
  }

  public boolean isNoOp() {
    return clientRequestId == null && NO_OP_VALUE.equals(value);
  }

  public boolean isGenesis() {
    return clientRequestId == null && GENESIS_VALUE.equals(value);
  }
}
