package pt.ulisboa.depchain.shared.network.links.stubborn.model;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;
import static pt.ulisboa.depchain.shared.utils.ValidationUtils.requireAllNonNull;

import java.util.Arrays;

import pt.ulisboa.depchain.shared.utils.ValidationUtils;

// Represents a message being tracked for potential retries in the stubborn link protocol.
public final class TrackedMessage {
  // Key to identify a message uniquely for tracking purposes.
  public record Key(long connectionId, int sequenceNumber, int messageTag) {
    public Key {
      ValidationUtils.requireInClosedRangeInt(messageTag, 0, 0xFF, "messageTag");
    }
  }

  private final Key key;
  private final byte[] payload;
  private final long createdAtMs;
  private int retryAttempt;
  private long nextRetryAtMs;

  public TrackedMessage(Key key, byte[] payload, int retryAttempt, long createdAtMs, long nextRetryAtMs) {
    requireAllNonNull(named("key", key), named("payload", payload));
    this.key = key;
    this.payload = Arrays.copyOf(payload, payload.length);
    this.retryAttempt = ValidationUtils.requireNonNegativeInt(retryAttempt, "retryAttempt");
    this.createdAtMs = ValidationUtils.requireNonNegativeLong(createdAtMs, "createdAtMs");
    this.nextRetryAtMs = ValidationUtils.requireAtLeast(nextRetryAtMs, this.createdAtMs, "nextRetryAtMs", "createdAtMs");
  }

  public void markRetried(long newNextRetryAtMs) {
    ValidationUtils.requireAtLeast(newNextRetryAtMs, createdAtMs, "newNextRetryAtMs", "createdAtMs");
    retryAttempt++;
    nextRetryAtMs = newNextRetryAtMs;
  }

  public Key key() {
    return key;
  }

  public byte[] payload() {
    return payload;
  }

  public int retryAttempt() {
    return retryAttempt;
  }

  public long createdAtMs() {
    return createdAtMs;
  }

  public long nextRetryAtMs() {
    return nextRetryAtMs;
  }
}
