package pt.ulisboa.depchain.shared.network.links.authenticated;

public enum AuthOpcode {
  INIT((byte) 1), REPLY((byte) 2), DATA((byte) 3);

  private final byte code;

  AuthOpcode(byte code) {
    this.code = code;
  }

  public byte code() {
    return code;
  }

  public static AuthOpcode fromCode(byte code) {
    for (AuthOpcode opcode : values()) {
      if (opcode.code == code) {
        return opcode;
      }
    }

    throw new IllegalArgumentException("Unsupported authentication opcode: " + Byte.toUnsignedInt(code));
  }
}
