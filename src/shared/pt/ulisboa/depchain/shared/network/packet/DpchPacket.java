package pt.ulisboa.depchain.shared.network.packet;

import java.io.IOException;
import java.util.Arrays;

import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class DpchPacket {
  public static final int MAX_PAYLOAD_LENGTH = 0xFFFF;
  public static final int MAX_PACKET_NUMBER = 0xFFFF;

  // DPCH packet structure:
  private final long connectionId;
  private final int flags;
  private final int sequenceNumber;
  private final byte[] payload;

  // Creates a DPCH from type without ACK.
  public static DpchPacket from(long connectionId, DpchType type, int sequenceNumber, byte[] payload) {
    return from(connectionId, type, false, sequenceNumber, payload);
  }

  // Creates a DPCH from type combining with ACK if requested.
  public static DpchPacket from(long connectionId, DpchType type, boolean withAck, int sequenceNumber, byte[] payload) {
    ValidationUtils.requireNonNull(type, "type");

    int flags;
    if (withAck) {
      if (type != DpchType.SYN && type != DpchType.FIN) {
        throw new IllegalArgumentException("ACK combination only supported for SYN/FIN for now");
      }
      flags = DpchFlags.withAck(type);
    } else {
      flags = DpchFlags.fromType(type);
    }

    return new DpchPacket(connectionId, flags, sequenceNumber, payload, true);
  }

  // Private constructor to control the copying of the payload.
  private DpchPacket(long connectionId, int flags, int sequenceNumber, byte[] payload, boolean copyPayload) {
    ValidationUtils.requireNonNull(payload, "payload");
    ValidationUtils.requireInClosedRangeInt(payload.length, 0, MAX_PAYLOAD_LENGTH, "payload.length");
    ValidationUtils.requireInClosedRangeInt(sequenceNumber, 0, MAX_PACKET_NUMBER, "sequenceNumber");

    this.connectionId = connectionId;
    this.flags = validateFlags(flags);
    this.sequenceNumber = sequenceNumber;
    if (copyPayload) {
      this.payload = Arrays.copyOf(payload, payload.length);
    } else {
      this.payload = payload;
    }
  }

  // Checks if the DPCH has the specified type, considering both reliable and ACK types.
  public boolean hasType(DpchType type) {
    ValidationUtils.requireNonNull(type, "type");
    return DpchFlags.hasType(flags, type);
  }

  // Returns the reliable type if the DPCH is reliable, or null if it is an ACK.
  public DpchType reliableTypeOrNull() {
    return DpchFlags.reliableTypeOrNull(flags);
  }

  public long connectionId() {
    return connectionId;
  }

  public int flags() {
    return flags;
  }

  public byte[] payload() {
    return Arrays.copyOf(payload, payload.length);
  }

  public int sequenceNumber() {
    return sequenceNumber;
  }

  public DpchType type() {
    DpchType reliable = DpchFlags.reliableTypeOrNull(flags);
    if (reliable != null) {
      return reliable;
    }
    return DpchType.ACK; // If it's not a reliable type, it's an ACK.
  }

  // Fast path for decoder (without copying the payload)
  static DpchPacket fromDecoded(long connectionId, int flags, int sequenceNumber, byte[] payload) {
    return new DpchPacket(connectionId, flags, sequenceNumber, payload, false);
  }

  // Internal read-only use to void repeated copying during serialization.
  byte[] payloadInternal() {
    return payload;
  }

  // Validates the flags and decodes them to ensure they are well-formed.
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
