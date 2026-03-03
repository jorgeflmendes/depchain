package pt.ulisboa.depchain.shared.network.dpch;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

import pt.ulisboa.depchain.shared.utils.BinarySerialization;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class DpchSerialization {
  // 2-byte DPCH-L protocol signature.
  private static final byte MAGIC_HI = 0x44; // 'D'
  private static final byte MAGIC_LO = 0x50; // 'P'

  // 1-byte protocol version.
  private static final byte FORMAT_VERSION = 2;
  static final int LONG_HEADER_SIZE = 14;

  public static byte[] toBytes(Dpch value) throws IOException {
    Objects.requireNonNull(value, "value cannot be null");

    try (ByteArrayOutputStream output = new ByteArrayOutputStream(); DataOutputStream dataOutput = new DataOutputStream(output)) {
      writeHeader(dataOutput, value);
      dataOutput.flush();
      return output.toByteArray();
    }
  }

  // Decode a message record from raw bytes.
  public static Dpch fromBytes(byte[] bytes, int offset, int length) throws IOException {
    ValidationUtils.requireValidSlice(bytes, offset, length);
    if (length < LONG_HEADER_SIZE) {
      throw new IOException("DPCH-L packet shorter than long header (" + length + " < " + LONG_HEADER_SIZE + ")");
    }

    try (ByteArrayInputStream input = new ByteArrayInputStream(bytes, offset, length); DataInputStream dataInput = new DataInputStream(input)) {
      // Read the magic number and version for basic sanity checks.
      byte magicHi = BinarySerialization.readByte(dataInput);
      byte magicLo = BinarySerialization.readByte(dataInput);
      if (magicHi != MAGIC_HI || magicLo != MAGIC_LO) {
        throw new IOException("Invalid message magic");
      }
      byte version = BinarySerialization.readByte(dataInput);
      if (version != FORMAT_VERSION) {
        throw new IOException("Unsupported message version: " + version);
      }

      // Remaining header fields.
      byte flagsByte = BinarySerialization.readByte(dataInput);
      int flags = DpchFlags.decode(flagsByte);
      long connectionId = BinarySerialization.readLong(dataInput);
      int sequenceNumber = Short.toUnsignedInt(BinarySerialization.readShort(dataInput));
      byte[] payload = dataInput.readNBytes(dataInput.available());

      // Check for trailing bytes, which would indicate a malformed packet.
      if (dataInput.available() != 0) {
        throw new IOException("Trailing bytes after packet payload");
      }

      return Dpch.decoded(connectionId, flags, sequenceNumber, payload);
    }
  }

  // Write full DPCH packet header and payload.
  private static void writeHeader(DataOutputStream output, Dpch packet) throws IOException {
    byte[] payload = packet.payloadInternal();

    BinarySerialization.writeByte(output, MAGIC_HI);
    BinarySerialization.writeByte(output, MAGIC_LO);
    BinarySerialization.writeByte(output, FORMAT_VERSION);
    BinarySerialization.writeByte(output, DpchFlags.encode(packet.flags()));
    BinarySerialization.writeLong(output, packet.connectionId());
    BinarySerialization.writeShort(output, (short) packet.sequenceNumber());

    output.write(payload);
  }
}
