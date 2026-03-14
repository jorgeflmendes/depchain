package pt.ulisboa.depchain.shared.network.links;

public final class LinkClosedException extends IllegalStateException {
  public LinkClosedException(String message) {
    super(message);
  }
}
