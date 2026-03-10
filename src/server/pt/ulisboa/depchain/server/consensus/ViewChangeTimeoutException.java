package pt.ulisboa.depchain.server.consensus;

// Pacemaker timeout exception.
public final class ViewChangeTimeoutException extends RuntimeException {
  public ViewChangeTimeoutException(String message) {
    super(message);
  }
}
