package pt.ulisboa.depchain.shared.network.links.stubborn.model;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.net.InetSocketAddress;

import pt.ulisboa.depchain.shared.utils.ValidationUtils;

// Represents a retry of a message that is scheduled to be sent at a specific time in the future.
public record ScheduledRetry(InetSocketAddress endpoint, TrackedMessage.Key messageKey, long dueAtMs) {
  public ScheduledRetry {
    ValidationUtils.requireAllNonNull(named("endpoint", endpoint), named("messageKey", messageKey));
    ValidationUtils.requireNonNegativeLong(dueAtMs, "dueAtMs");
  }
}
