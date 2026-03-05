package pt.ulisboa.depchain.shared.network.dpch;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class DpchSerialization {
  // Header size in bytes (magic hi [1] + magic lo [1] + version [1] + flags [1] + connection id [8] + sequence number [2]).
  public static final int HEADER_SIZE = 14;

  // 2-byte DPCH protocol signature.
  private static final byte MAGIC_HI = 0x44; // 'D'
  private static final byte MAGIC_LO = 0x50; // 'P'

  // 1-byte protocol version.
  private static final byte FORMAT_VERSION = 1;

  // Message record into raw bytes.
  public static byte[] toBytes(Dpch value) throws IOException {
    ValidationUtils.requireNonNull(value, "value");

    try (ByteArrayOutputStream output = new ByteArrayOutputStream(); DataOutputStream dataOutput = new DataOutputStream(output)) {
      writeHeader(dataOutput, value);
      dataOutput.flush();
      return output.toByteArray();
    }
  }

  // Raw bytes to message record with validation.
  public static Dpch fromBytes(byte[] bytes, int offset, int length) throws IOException {
    ValidationUtils.requireValidSlice(bytes, offset, length);
    if (length < HEADER_SIZE) {
      throw new IOException("DPCH packet shorter than header (" + length + " < " + HEADER_SIZE + ")");
    }

    try (ByteArrayInputStream input = new ByteArrayInputStream(bytes, offset, length); DataInputStream dataInput = new DataInputStream(input)) {
      // Read the magic number and version for basic sanity checks.
      byte magicHi = dataInput.readByte();
      byte magicLo = dataInput.readByte();
      if (magicHi != MAGIC_HI || magicLo != MAGIC_LO) {
        throw new IOException("Invalid message magic");
      }
      byte version = dataInput.readByte();
      if (version != FORMAT_VERSION) {
        throw new IOException("Unsupported message version: " + version);
      }

      // Remaining header fields.
      byte flagsByte = dataInput.readByte();
      int flags = DpchFlags.decode(flagsByte);
      long connectionId = dataInput.readLong();
      int sequenceNumber = Short.toUnsignedInt(dataInput.readShort());
      int payloadLength = length - HEADER_SIZE;
      byte[] payload = dataInput.readNBytes(payloadLength);

      return Dpch.fromDecoded(connectionId, flags, sequenceNumber, payload);
    }
  }

  // Write full DPCH packet header and payload.
  private static void writeHeader(DataOutputStream output, Dpch packet) throws IOException {
    byte[] payload = packet.payloadInternal();

    output.writeByte(MAGIC_HI);
    output.writeByte(MAGIC_LO);
    output.writeByte(FORMAT_VERSION);
    output.writeByte(DpchFlags.encode(packet.flags()));
    output.writeLong(packet.connectionId());
    output.writeShort(packet.sequenceNumber());

    output.write(payload);
  }
}
