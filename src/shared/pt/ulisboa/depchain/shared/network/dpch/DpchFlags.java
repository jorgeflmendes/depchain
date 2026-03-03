package pt.ulisboa.depchain.shared.network.dpch;

import java.io.IOException;

final class DpchFlags {
  static final int DATA = 1 << 0;
  static final int ACK = 1 << 1;
  static final int SYN = 1 << 2;
  static final int FIN = 1 << 3;
  static final int CTRL = 1 << 4;
  static final int RESERVED_MASK = 0b1110_0000;

  private DpchFlags() {
    // Utility class.
  }

  static int fromType(DpchType type) {
    return switch (type) {
      case DATA -> DATA;
      case ACK -> ACK;
      case SYN -> SYN | CTRL;
      case FIN -> FIN | CTRL;
    };
  }

  static int decode(byte flagsByte) throws IOException {
    int flags = Byte.toUnsignedInt(flagsByte);
    if ((flags & RESERVED_MASK) != 0) {
      throw new IOException("Unsupported reserved DPCH flags bits set: " + Integer.toBinaryString(flags));
    }

    int semanticBits = flags & (DATA | ACK | SYN | FIN);
    if (semanticBits == 0) {
      throw new IOException("Unsupported DPCH flags combination: " + Integer.toBinaryString(flags));
    }

    int reliableBits = flags & (DATA | SYN | FIN);
    if (Integer.bitCount(reliableBits) > 1) {
      throw new IOException("Unsupported DPCH reliable flags combination: " + Integer.toBinaryString(flags));
    }

    return flags;
  }

  static byte encode(int flags) {
    return (byte) flags;
  }

  static boolean hasType(int flags, DpchType type) {
    return (flags & fromType(type)) != 0;
  }

  static DpchType reliableTypeOrNull(int flags) {
    int reliableBits = flags & (DATA | SYN | FIN);
    return switch (reliableBits) {
      case DATA -> DpchType.DATA;
      case SYN -> DpchType.SYN;
      case FIN -> DpchType.FIN;
      default -> null;
    };
  }

  static int withAck(DpchType reliableType) {
    return fromType(reliableType) | ACK;
  }
}
