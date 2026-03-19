package pt.ulisboa.depchain.server.consensus;

public final class ConsensusTimeoutException extends RuntimeException {
  public ConsensusTimeoutException(String message) {
    super(message);
  }
}
