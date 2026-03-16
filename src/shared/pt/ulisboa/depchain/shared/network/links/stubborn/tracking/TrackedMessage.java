package pt.ulisboa.depchain.shared.network.links.stubborn.tracking;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.util.Arrays;

import pt.ulisboa.depchain.shared.network.links.RunOnce;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

// Represents a message being tracked for potential retries.
public final class TrackedMessage {
  private final TrackedKey key;
  private final byte[] payload;
  private final long createdAtMs;
  private final RunOnce onTerminal;
  private int retryAttempt;
  private long nextRetryAtMs;

  public TrackedMessage(TrackedKey key, byte[] payload, int retryAttempt, long createdAtMs, long nextRetryAtMs) {
    this(key, payload, retryAttempt, createdAtMs, nextRetryAtMs, () -> {
    });
  }

  public TrackedMessage(TrackedKey key, byte[] payload, int retryAttempt, long createdAtMs, long nextRetryAtMs, Runnable onTerminal) {
    ValidationUtils.requireAllNonNull(named("key", key), named("payload", payload));
    this.key = key;
    this.payload = Arrays.copyOf(payload, payload.length);
    this.retryAttempt = ValidationUtils.requireNonNegativeInt(retryAttempt, "retryAttempt");
    this.createdAtMs = ValidationUtils.requireNonNegativeLong(createdAtMs, "createdAtMs");
    this.nextRetryAtMs = ValidationUtils.requireAtLeast(nextRetryAtMs, this.createdAtMs, "nextRetryAtMs", "createdAtMs");
    this.onTerminal = new RunOnce(ValidationUtils.requireNonNull(onTerminal, "onTerminal"));
  }

  public void markRetried(long newNextRetryAtMs) {
    ValidationUtils.requireAtLeast(newNextRetryAtMs, createdAtMs, "newNextRetryAtMs", "createdAtMs");
    retryAttempt++;
    nextRetryAtMs = newNextRetryAtMs;
  }

  public TrackedKey key() {
    return key;
  }

  public byte[] payload() {
    return Arrays.copyOf(payload, payload.length);
  }

  public byte[] payloadView() {
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

  public void notifyTerminalState() {
    onTerminal.run();
  }
}
