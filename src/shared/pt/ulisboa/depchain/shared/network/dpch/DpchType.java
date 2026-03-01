package pt.ulisboa.depchain.shared.network.dpch;

public enum DpchType {
  DATA((byte) 0), // DPCH packet containing data
  ACK((byte) 1), // DPCH packet acknowledging receipt of a DATA packet
  NACK((byte) 2), // DPCH packet indicating that a DATA packet was not received correctly
  SYN((byte) 3), // DPCH packet used to synchronize the state of the sender and receiver
  FIN((byte) 4); // DPCH packet used to indicate the end of a communication session

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
