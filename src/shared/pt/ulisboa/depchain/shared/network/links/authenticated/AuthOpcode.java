package pt.ulisboa.depchain.shared.network.links.authenticated;

// Enum representing the opcodes for authenticated link messages.
public enum AuthOpcode {
  INIT((byte) 1), REPLY((byte) 2), DATA((byte) 3);

  private final byte code;
  private static final AuthOpcode[] BY_UNSIGNED_CODE = buildLookup();

  AuthOpcode(byte code) {
    this.code = code;
  }

  public byte code() {
    return code;
  }

  public static AuthOpcode fromCode(byte code) {
    int unsignedCode = Byte.toUnsignedInt(code);
    AuthOpcode opcode = BY_UNSIGNED_CODE[unsignedCode];
    if (opcode == null) {
      throw new IllegalArgumentException("Unsupported authentication opcode: " + unsignedCode);
    }
    return opcode;
  }

  private static AuthOpcode[] buildLookup() {
    AuthOpcode[] lookup = new AuthOpcode[256];
    for (AuthOpcode opcode : values()) {
      lookup[Byte.toUnsignedInt(opcode.code)] = opcode;
    }
    return lookup;
  }
}

