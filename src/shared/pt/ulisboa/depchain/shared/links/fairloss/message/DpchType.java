package pt.ulisboa.depchain.shared.links.fairloss.message;

public enum DpchType {
  DATA((byte) 0),
  ACK((byte) 1),
  NACK((byte) 2),
  SYN((byte) 3),
  FIN((byte) 4);

  private final byte code;

  DpchType(byte code) {
    this.code = code;
  }

  public byte code() {
    return code;
  }

  public static DpchType fromCode(byte code) {
    for (DpchType type : values()) {
      if (type.code == code) {
        return type;
      }
    }

    throw new IllegalArgumentException("Unsupported DPCH packet type: " + Byte.toUnsignedInt(code));
  }
}
