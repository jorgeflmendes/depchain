package pt.ulisboa.depchain.shared.network.dpch;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public final class Dpch {
  // Keep payload bounded to uint16-sized datagrams.
  public static final int MAX_PAYLOAD_LENGTH = 0xFFFF;
  public static final int MAX_PACKET_NUMBER = 0xFFFF;

  // DPCH packet structure:
  private final long connectionId;
  private final int flags;
  private final int sequenceNumber;
  private final byte[] payload;

  public Dpch(long connectionId, DpchType type, int sequenceNumber, byte[] payload) {
    this(connectionId, DpchFlags.fromType(type), sequenceNumber, payload);
  }

  public Dpch(long connectionId, int flags, int sequenceNumber, byte[] payload) {
    this(connectionId, validateFlags(flags), sequenceNumber, payload, true);
  }

  private Dpch(long connectionId, int flags, int sequenceNumber, byte[] payload, boolean copyPayload) {
    Objects.requireNonNull(payload, "payload cannot be null");
    if (payload.length > MAX_PAYLOAD_LENGTH) {
      throw new IllegalArgumentException("payload length exceeds uint16 max (%d > %d)".formatted(payload.length, MAX_PAYLOAD_LENGTH));
    }
    if (sequenceNumber < 0 || sequenceNumber > MAX_PACKET_NUMBER) {
      throw new IllegalArgumentException("sequenceNumber must be in [0, %d]".formatted(MAX_PACKET_NUMBER));
    }

    this.connectionId = connectionId;
    this.flags = flags;
    this.sequenceNumber = sequenceNumber;
    this.payload = copyPayload ? Arrays.copyOf(payload, payload.length) : payload;
  }

  public static Dpch data(long connectionId, int sequenceNumber, byte[] payload) {
    return new Dpch(connectionId, DpchType.DATA, sequenceNumber, payload);
  }

  public static Dpch ack(long connectionId, int sequenceNumber, byte[] payload) {
    return new Dpch(connectionId, DpchType.ACK, sequenceNumber, payload);
  }

  public static Dpch syn(long connectionId, int sequenceNumber, byte[] payload) {
    return new Dpch(connectionId, DpchType.SYN, sequenceNumber, payload);
  }

  public static Dpch synAck(long connectionId, int sequenceNumber, byte[] payload) {
    return new Dpch(connectionId, DpchFlags.withAck(DpchType.SYN), sequenceNumber, payload);
  }

  public static Dpch fin(long connectionId, int sequenceNumber, byte[] payload) {
    return new Dpch(connectionId, DpchType.FIN, sequenceNumber, payload);
  }

  public static Dpch finAck(long connectionId, int sequenceNumber, byte[] payload) {
    return new Dpch(connectionId, DpchFlags.withAck(DpchType.FIN), sequenceNumber, payload);
  }

  // Fast path for decoder: payload already belongs exclusively to this packet instance.
  static Dpch decoded(long connectionId, int flags, int sequenceNumber, byte[] payload) {
    return new Dpch(connectionId, flags, sequenceNumber, payload, false);
  }

  public long connectionId() {
    return connectionId;
  }

  public int flags() {
    return flags;
  }

  public DpchType type() {
    DpchType reliable = DpchFlags.reliableTypeOrNull(flags);
    if (reliable != null) {
      return reliable;
    }
    return DpchType.ACK;
  }

  public boolean hasType(DpchType type) {
    Objects.requireNonNull(type, "type cannot be null");
    return DpchFlags.hasType(flags, type);
  }

  public DpchType reliableTypeOrNull() {
    return DpchFlags.reliableTypeOrNull(flags);
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

  private static int validateFlags(int flags) {
    if ((flags & ~0xFF) != 0) {
      throw new IllegalArgumentException("DPCH flags must be an unsigned byte");
    }

    try {
      return DpchFlags.decode((byte) flags);
    } catch (IOException exception) {
      throw new IllegalArgumentException(exception.getMessage(), exception);
    }
  }
}
