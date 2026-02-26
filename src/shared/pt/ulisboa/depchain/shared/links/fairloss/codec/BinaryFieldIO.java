package pt.ulisboa.depchain.shared.links.fairloss.codec;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

public final class BinaryFieldIO {
  // Write a boolean value to the output stream.
  public static void writeBoolean(DataOutputStream output, boolean value) throws IOException {
    output.writeBoolean(value);
  }

  // Read a boolean value from the input stream.
  public static boolean readBoolean(DataInputStream input) throws IOException {
    return input.readBoolean();
  }

  // Write a byte value to the output stream.
  public static void writeByte(DataOutputStream output, byte value) throws IOException {
    output.writeByte(value);
  }

  // Read a byte value from the input stream.
  public static byte readByte(DataInputStream input) throws IOException {
    return input.readByte();
  }

  // Write a short value to the output stream.
  public static void writeShort(DataOutputStream output, short value) throws IOException {
    output.writeShort(value);
  }

  // Read a short value from the input stream.
  public static short readShort(DataInputStream input) throws IOException {
    return input.readShort();
  }

  // Write an int value to the output stream.
  public static void writeInt(DataOutputStream output, int value) throws IOException {
    output.writeInt(value);
  }

  // Read an int value from the input stream.
  public static int readInt(DataInputStream input) throws IOException {
    return input.readInt();
  }

  // Write a long value to the output stream.
  public static void writeLong(DataOutputStream output, long value) throws IOException {
    output.writeLong(value);
  }

  // Read a long value from the input stream.
  public static long readLong(DataInputStream input) throws IOException {
    return input.readLong();
  }

  // Write a float value to the output stream.
  public static void writeFloat(DataOutputStream output, float value) throws IOException {
    output.writeFloat(value);
  }

  // Read a float value from the input stream.
  public static float readFloat(DataInputStream input) throws IOException {
    return input.readFloat();
  }

  // Write a double value to the output stream.
  public static void writeDouble(DataOutputStream output, double value) throws IOException {
    output.writeDouble(value);
  }

  // Read a double value from the input stream.
  public static double readDouble(DataInputStream input) throws IOException {
    return input.readDouble();
  }

  // Write a char to the output stream.
  public static void writeChar(DataOutputStream output, char value) throws IOException {
    output.writeChar(value);
  }

  // Read a char from the input stream.
  public static char readChar(DataInputStream input) throws IOException {
    return input.readChar();
  }

  // Write a UUID to the output stream by writing its most significant bits and least significant bits as two longs.
  public static void writeUuid(DataOutputStream output, UUID value) throws IOException {
    Objects.requireNonNull(value, "value cannot be null");
    writeLong(output, value.getMostSignificantBits());
    writeLong(output, value.getLeastSignificantBits());
  }

  // Read a UUID from the input stream by reading two longs (most significant bits and least significant bits) and constructing a UUID object.
  public static UUID readUuid(DataInputStream input) throws IOException {
    long msb = readLong(input);
    long lsb = readLong(input);
    return new UUID(msb, lsb);
  }

  // Write a byte array to the output stream, first writing the length as an int and then the bytes.
  public static void writeBytes(DataOutputStream output, byte[] value) throws IOException {
    Objects.requireNonNull(value, "value cannot be null");
    writeInt(output, value.length);
    output.write(value);
  }

  // Read a byte array from the input stream, first reading the length as an int and then the bytes.
  public static byte[] readBytes(DataInputStream input) throws IOException {
    int length = readInt(input);
    if (length < 0) {
      throw new IOException("Invalid byte array length");
    }

    if (length > input.available()) {
      throw new IOException("Invalid byte array length for packet size");
    }

    byte[] data = input.readNBytes(length);
    if (data.length != length) {
      throw new IOException("Incomplete byte array payload");
    }
    return data;
  }

  // Write a UTF-8 encoded string to the output stream, first writing the length as an int and then the bytes.
  public static void writeString(DataOutputStream output, String value) throws IOException {
    Objects.requireNonNull(value, "value cannot be null");
    writeBytes(output, value.getBytes(StandardCharsets.UTF_8));
  }

  // Read a UTF-8 encoded string from the input stream, first reading the length as an int and then the bytes.
  public static String readString(DataInputStream input) throws IOException {
    return new String(readBytes(input), StandardCharsets.UTF_8);
  }
}
