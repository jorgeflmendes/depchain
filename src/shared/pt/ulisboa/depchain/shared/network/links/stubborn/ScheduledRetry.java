package pt.ulisboa.depchain.shared.network.links.stubborn;

import java.net.InetSocketAddress;
import java.util.Objects;

import pt.ulisboa.depchain.shared.utils.ValidationUtils;

// Represents a scheduled retry for a tracked message
record ScheduledRetry(InetSocketAddress endpoint, TrackedMessage.Key messageKey, long dueAtMs) {
  ScheduledRetry {
    Objects.requireNonNull(endpoint, "endpoint cannot be null");
    Objects.requireNonNull(messageKey, "messageKey cannot be null");
    ValidationUtils.requireNonNegativeLong(dueAtMs, "dueAtMs");
  }
}
