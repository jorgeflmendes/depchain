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
  // 4-byte DPCH protocol signature.
  private static final int MAGIC_NUMBER = 0x44504348; // "DPCH" in ASCII

  // 1-byte protocol version.
  private static final byte FORMAT_VERSION = 1;

  // Encode only allow-listed message records to avoid unsafe object deserialization.
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
    validateSlice(bytes, offset, length);

    try (ByteArrayInputStream input = new ByteArrayInputStream(bytes, offset, length); DataInputStream dataInput = new DataInputStream(input)) {
      
      // Read and validate the DPCH protocol header (magic number and version).
      int magic = BinarySerialization.readInt(dataInput);
      if (magic != MAGIC_NUMBER) {
        throw new IOException("Invalid message magic");
      }

      byte version = BinarySerialization.readByte(dataInput);
      if (version != FORMAT_VERSION) {
        throw new IOException("Unsupported message version: " + version);
      }

      // Read the rest of the packet fields and payload.
      int connectionId = BinarySerialization.readInt(dataInput);
      byte typeCode = BinarySerialization.readByte(dataInput);
      int sequenceNumber = BinarySerialization.readInt(dataInput);
      int payloadLength = Short.toUnsignedInt(BinarySerialization.readShort(dataInput));

      if (payloadLength > dataInput.available()) {
        throw new IOException("Invalid payload length for packet size");
      }

      byte[] payload = dataInput.readNBytes(payloadLength);
      if (payload.length != payloadLength) {
        throw new IOException("Incomplete packet payload");
      }

      if (dataInput.available() != 0) {
        throw new IOException("Trailing bytes after packet payload");
      }

      try {
        DpchType type = DpchType.fromCode(typeCode);
        return new Dpch(connectionId, type, sequenceNumber, payload);
      } catch (IllegalArgumentException invalidPacket) {
        throw new IOException(invalidPacket.getMessage(), invalidPacket);
      }
    }
  }

  // Validate the provided byte array slice before decoding untrusted data.
  private static void validateSlice(byte[] bytes, int offset, int length) {
    ValidationUtils.requireValidSlice(bytes, offset, length);
  }

  // Write full DPCH packet header and payload.
  private static void writeHeader(DataOutputStream output, Dpch packet) throws IOException {
    byte[] payload = packet.payload();

    BinarySerialization.writeInt(output, MAGIC_NUMBER);
    BinarySerialization.writeByte(output, FORMAT_VERSION);
    BinarySerialization.writeInt(output, packet.connectionId());
    BinarySerialization.writeByte(output, packet.type().code());
    BinarySerialization.writeInt(output, packet.sequenceNumber());
    BinarySerialization.writeShort(output, (short) payload.length);

    output.write(payload);
  }
}
