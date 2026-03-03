package pt.ulisboa.depchain.shared.network.links.stubborn;

import java.util.Objects;

import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.dpch.DpchType;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class TrackedMessage {
  // Public identity handle used by callers to manage tracked sends.
  public record Key(long connectionId, int sequenceNumber, DpchType messageType) {
    public Key {
      Objects.requireNonNull(messageType, "messageType cannot be null");
    }

    // Build the tracking key from packet identity fields.
    public static Key fromPacket(Dpch packet) {
      Objects.requireNonNull(packet, "packet cannot be null");
      return new Key(packet.connectionId(), packet.sequenceNumber(), packet.type());
    }
  }

  // Identity of the tracked message.
  private final Key key;

  // The actual message packet being tracked.
  private final Dpch packet;

  // Timestamp of when this tracked message was created (first sent).
  private final long createdAtMs;

  // Number of retry attempts made so far (0 for the initial send).
  private int retryAttempt;

  // Timestamp of when the next retry attempt should be made.
  private long nextRetryAtMs;

  TrackedMessage(Key key, Dpch packet, int retryAttempt, long createdAtMs, long nextRetryAtMs) {
    this.key = Objects.requireNonNull(key, "key cannot be null");
    this.packet = Objects.requireNonNull(packet, "packet cannot be null");
    this.retryAttempt = ValidationUtils.requireNonNegativeInt(retryAttempt, "retryAttempt");
    this.createdAtMs = ValidationUtils.requireNonNegativeLong(createdAtMs, "createdAtMs");
    this.nextRetryAtMs = ValidationUtils.requireAtLeast(nextRetryAtMs, this.createdAtMs, "nextRetryAtMs", "createdAtMs");
  }

  // Marks this tracked message as having been retried, incrementing the retry attempt count and updating the next retry timestamp.
  void markRetried(long newNextRetryAtMs) {
    ValidationUtils.requireAtLeast(newNextRetryAtMs, createdAtMs, "newNextRetryAtMs", "createdAtMs");
    retryAttempt++;
    nextRetryAtMs = newNextRetryAtMs;
  }

  Key key() {
    return key;
  }

  Dpch packet() {
    return packet;
  }

  int retryAttempt() {
    return retryAttempt;
  }

  long createdAtMs() {
    return createdAtMs;
  }

  long nextRetryAtMs() {
    return nextRetryAtMs;
  }

}
