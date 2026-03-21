package pt.ulisboa.depchain.shared.network.links.stubborn.tracking;

import java.util.Objects;

import org.eclipse.jdt.annotation.Nullable;

import pt.ulisboa.depchain.shared.network.links.RunOnce;

// Represents a message being tracked for potential retries.
public final class TrackedMessage {
  private final TrackedKey key;
  private final byte[] payload;
  private final @Nullable RunOnce onTerminal;
  private int retryAttempt;
  private long nextRetryAtNanos;

  public TrackedMessage(TrackedKey key, byte[] payload) {
    Objects.requireNonNull(key, "key cannot be null");
    Objects.requireNonNull(payload, "payload cannot be null");
    this.key = key;
    this.payload = payload;
    this.retryAttempt = 0;
    this.onTerminal = null;
  }

  public TrackedMessage(TrackedKey key, byte[] payload, Runnable onTerminal) {
    Objects.requireNonNull(key, "key cannot be null");
    Objects.requireNonNull(payload, "payload cannot be null");
    this.key = key;
    this.payload = payload;
    this.retryAttempt = 0;
    this.onTerminal = new RunOnce(Objects.requireNonNull(onTerminal, "onTerminal cannot be null"));
  }

  public void advanceRetryAttempt() {
    retryAttempt++;
  }

  public TrackedKey key() {
    return key;
  }

  public byte[] payload() {
    return payload.clone();
  }

  public byte[] payloadView() {
    return payload;
  }

  public int retryAttempt() {
    return retryAttempt;
  }

  public long nextRetryAtNanos() {
    return nextRetryAtNanos;
  }

  public void scheduleRetryAfterNanos(long delayNanos) {
    nextRetryAtNanos = System.nanoTime() + Math.max(1L, delayNanos);
  }

  public void notifyTerminalState() {
    if (onTerminal != null) {
      onTerminal.run();
    }
  }
}
