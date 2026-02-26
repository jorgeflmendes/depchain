package pt.ulisboa.depchain.shared.links.fairloss.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

import pt.ulisboa.depchain.shared.links.fairloss.message.FairLossLinkMessage;
import pt.ulisboa.depchain.shared.links.fairloss.message.FairLossRequestMessage;
import pt.ulisboa.depchain.shared.links.fairloss.message.FairLossResponseMessage;

public final class FairLossPacketCodec {
  // Packet Magic Number: 4 bytes used to identify the start of a valid message and prevent deserialization of random data.
  private static final int PACKET_MAGIC_NUMBER = 0x44504348; // "DPCH" in ASCII

  // Packet Format Version: 1 byte used to indicate the version of the packet format for future compatibility checks.
  private static final byte PACKET_FORMAT_VERSION = 1;
  
  // List of supported message codecs for encoding/decoding messages, ordered by message type ID.
  private static final List<FairLossMessageCodec<? extends FairLossLinkMessage>> MESSAGE_CODECS = List.of(FairLossRequestMessage.CODEC, FairLossResponseMessage.CODEC);

  // Encode only allow-listed message records to avoid unsafe object deserialization.
  public static byte[] toBytes(FairLossLinkMessage value) throws IOException {
    Objects.requireNonNull(value, "value cannot be null");
    FairLossMessageCodec<?> codec = codecForValue(value);

    try (ByteArrayOutputStream output = new ByteArrayOutputStream(); DataOutputStream dataOutput = new DataOutputStream(output)) {
      writeHeader(dataOutput, codec.messageTypeId());
      writeMessageBody(codec, value, dataOutput);
      dataOutput.flush();
      return output.toByteArray();
    }
  }

  // Decode a message record from raw bytes.
  public static FairLossLinkMessage fromBytes(byte[] bytes, int offset, int length) throws IOException {
    validateSlice(bytes, offset, length);

    try (ByteArrayInputStream input = new ByteArrayInputStream(bytes, offset, length);
        DataInputStream dataInput = new DataInputStream(input)) {
      int magic = dataInput.readInt();
      if (magic != PACKET_MAGIC_NUMBER) {
        throw new IOException("Invalid message magic");
      }

      byte version = dataInput.readByte();
      if (version != PACKET_FORMAT_VERSION) {
        throw new IOException("Unsupported message version: " + version);
      }

      byte messageTypeId = dataInput.readByte();
      FairLossMessageCodec<?> codec = codecForMessageTypeId(messageTypeId);
      FairLossLinkMessage message = codec.readBody(dataInput);

      if (dataInput.available() != 0) {
        throw new IOException("Trailing bytes after message payload");
      }

      return message;
    }
  }

  // Validate the provided byte array slice before decoding untrusted data.
  private static void validateSlice(byte[] bytes, int offset, int length) {
    Objects.requireNonNull(bytes, "bytes cannot be null");
    if (offset < 0 || length < 0 || offset > bytes.length || (offset + length) > bytes.length) {
      throw new IllegalArgumentException("Invalid byte array slice");
    }
  }

  // Find the appropriate codec for the given message value, or throw if unsupported.
  private static FairLossMessageCodec<?> codecForValue(FairLossLinkMessage value) throws IOException {
    for (FairLossMessageCodec<?> codec : MESSAGE_CODECS) {
      if (codec.messageClass().isInstance(value)) {
        return codec;
      }
    }
    throw new IOException("Unsupported message type for serialization: " + value.getClass().getName());
  }

  // Find the appropriate codec for the given message type ID, or throw if unsupported.
  private static FairLossMessageCodec<?> codecForMessageTypeId(byte messageTypeId) throws IOException {
    for (FairLossMessageCodec<?> codec : MESSAGE_CODECS) {
      if (codec.messageTypeId() == messageTypeId) {
        return codec;
      }
    }
    throw new IOException("Unsupported message type: " + Byte.toUnsignedInt(messageTypeId));
  }

  // Helper method to write the message body using the appropriate codec.
  private static <T extends FairLossLinkMessage> void writeMessageBody(FairLossMessageCodec<T> codec, FairLossLinkMessage value, DataOutputStream output) throws IOException {
    codec.writeBody(codec.messageClass().cast(value), output);
  }

  // Write header used by all messages to support protocol validation/versioning.
  private static void writeHeader(DataOutputStream output, byte messageType) throws IOException {
    output.writeInt(PACKET_MAGIC_NUMBER);
    output.writeByte(PACKET_FORMAT_VERSION);
    output.writeByte(messageType);
  }
}
