package pt.ulisboa.depchain.shared.network.dpch;

public enum DpchType {
  // TCP-like packet types.
  DATA((byte) 0), ACK((byte) 1), SYN((byte) 3), FIN((byte) 4);

  private final byte code;

  // Lookup table for efficient byte to enum mapping.
  private static final DpchType[] BY_UNSIGNED_CODE = buildLookup();

  DpchType(byte code) {
    this.code = code;
  }

  public byte code() {
    return code;
  }

  // Maps a byte code to the corresponding enum value with validation
  public static DpchType fromCode(byte code) {
    int unsignedCode = Byte.toUnsignedInt(code);
    DpchType type = BY_UNSIGNED_CODE[unsignedCode];
    if (type == null) {
      throw new IllegalArgumentException("Unsupported DPCH packet type: " + unsignedCode);
    }
    return type;
  }

  // Pre-build a lookup table for byte to enum mapping.
  private static DpchType[] buildLookup() {
    DpchType[] lookup = new DpchType[256];
    for (DpchType type : values()) {
      lookup[Byte.toUnsignedInt(type.code)] = type;
    }
    return lookup;
  }
}
