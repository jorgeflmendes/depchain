package pt.ulisboa.depchain.shared.model;

import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public record ClientResponse(boolean success, String message) {
  public ClientResponse {
    ValidationUtils.requireNonNull(message, "message");
  }
}
