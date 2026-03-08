package pt.ulisboa.depchain.shared.network.links;

public class LinkFailureException extends RuntimeException {
  public LinkFailureException(String message) {
    super(message);
  }
}
