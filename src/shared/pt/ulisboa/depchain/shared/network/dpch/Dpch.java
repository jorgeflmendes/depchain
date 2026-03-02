package pt.ulisboa.depchain.shared.network.dpch;

import java.util.Arrays;
import java.util.Objects;

public final class Dpch {
  // DPCH packet payload length is encoded as uint16, so the maximum length is 0xFFFF (65535)
  public static final int MAX_PAYLOAD_LENGTH = 0xFFFF; // TODO: consider using a smaller max payload length to avoid fragmentation at the IP layer

  // DPCH packet structure:
  private final int connectionId;
  private final DpchType type;
  private final int sequenceNumber;
  private final byte[] payload;

  public Dpch(int connectionId, DpchType type, int sequenceNumber, byte[] payload) {
    this(connectionId, type, sequenceNumber, payload, true);
  }

  private Dpch(int connectionId, DpchType type, int sequenceNumber, byte[] payload, boolean copyPayload) {
    Objects.requireNonNull(type, "type cannot be null");
    Objects.requireNonNull(payload, "payload cannot be null");
    if (payload.length > MAX_PAYLOAD_LENGTH) {
      throw new IllegalArgumentException("payload length exceeds uint16 max (%d > %d)".formatted(payload.length, MAX_PAYLOAD_LENGTH));
    }

    this.connectionId = connectionId;
    this.type = type;
    this.sequenceNumber = sequenceNumber;
    this.payload = copyPayload ? Arrays.copyOf(payload, payload.length) : payload;
  }

  public static Dpch data(int connectionId, int sequenceNumber, byte[] payload) {
    return new Dpch(connectionId, DpchType.DATA, sequenceNumber, payload);
  }

  public static Dpch ack(int connectionId, int sequenceNumber, byte[] payload) {
    return new Dpch(connectionId, DpchType.ACK, sequenceNumber, payload);
  }

  public static Dpch syn(int connectionId, int sequenceNumber, byte[] payload) {
    return new Dpch(connectionId, DpchType.SYN, sequenceNumber, payload);
  }

  public static Dpch fin(int connectionId, int sequenceNumber, byte[] payload) {
    return new Dpch(connectionId, DpchType.FIN, sequenceNumber, payload);
  }

  // Fast path for decoder: payload already belongs exclusively to this packet instance.
  static Dpch decoded(int connectionId, DpchType type, int sequenceNumber, byte[] payload) {
    return new Dpch(connectionId, type, sequenceNumber, payload, false);
  }

  public int connectionId() {
    return connectionId;
  }

  public DpchType type() {
    return type;
  }

  public int sequenceNumber() {
    return sequenceNumber;
  }

  public byte[] payload() {
    return Arrays.copyOf(payload, payload.length);
  }

  // Internal read-only use inside the dpch package to avoid repeated copying during serialization.
  byte[] payloadInternal() {
    return payload;
  }
}
