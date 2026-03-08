package pt.ulisboa.depchain.shared.network.dpch;

import java.io.IOException;

final class DpchFlags {
  // 1st bit: DATA, 2nd bit: ACK, 3rd bit: SYN, 4th bit: FIN. No other combinations supported for now.
  static final int RESERVED_MASK = 0b1111_0000;
  static final int DATA = 1 << 0;
  static final int ACK = 1 << 1;
  static final int SYN = 1 << 2;
  static final int FIN = 1 << 3;

  private DpchFlags() {
  }

  // Enum to flags mapping (without ACK bit).
  static int fromType(DpchType type) {
    return switch (type) {
      case DATA -> DATA;
      case ACK -> ACK;
      case SYN -> SYN;
      case FIN -> FIN;
    };
  }

  // Flags to byte mapping for serialization.
  static byte encode(int flags) {
    return (byte) flags;
  }

  // Byte to flags mapping for deserialization with validation.
  static int decode(byte flagsByte) throws IOException {
    int flags = Byte.toUnsignedInt(flagsByte);
    if ((flags & RESERVED_MASK) != 0) {
      throw new IOException("Unsupported reserved DPCH flags bits set: " + Integer.toBinaryString(flags));
    }

    // At least one supported flag must be set.
    if (extractSupportedFlags(flags) == 0) {
      throw new IOException("Unsupported DPCH flags combination: " + Integer.toBinaryString(flags));
    }

    // Only one reliable type flag (DATA/SYN/FIN) can be set at a time, but ACK can be combined with any
    // of them.
    if (Integer.bitCount(extractReliableFlags(flags)) > 1) {
      throw new IOException("Unsupported DPCH reliable flags combination: " + Integer.toBinaryString(flags));
    }

    return flags;
  }

  // Checks if the given type's corresponding flag is set in the flags.
  static boolean hasType(int flags, DpchType type) {
    return switch (type) {
      case DATA -> (flags & DATA) != 0;
      case ACK -> (flags & ACK) != 0;
      case SYN -> (flags & SYN) != 0;
      case FIN -> (flags & FIN) != 0;
    };
  }

  // Extracts the reliable type (DATA/SYN/FIN) from the flags, or returns null.
  static DpchType reliableTypeOrNull(int flags) {
    return switch (extractReliableFlags(flags)) {
      case DATA -> DpchType.DATA;
      case SYN -> DpchType.SYN;
      case FIN -> DpchType.FIN;
      default -> null;
    };
  }

  // Add ACK flag to a reliable type (SYN/FIN) to create SYN-ACK/FIN-ACK combination.
  static int withAck(DpchType reliableType) {
    return fromType(reliableType) | ACK;
  }

  // Extracts all the supported flags
  static int extractSupportedFlags(int flags) {
    return flags & (DATA | ACK | SYN | FIN);
  }

  // Extracts only the reliable flags (no ACKs).
  static int extractReliableFlags(int flags) {
    return flags & (DATA | SYN | FIN);
  }
}
