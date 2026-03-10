package pt.ulisboa.depchain.shared.network.links;

// Exception indicating a failure in the link layer.
public class LinkFailureException extends RuntimeException {
  public LinkFailureException(String message) {
    super(message);
  }
}
