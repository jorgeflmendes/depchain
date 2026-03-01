package pt.ulisboa.depchain.shared.utils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class BinarySerializationTest {
  @Test
  void primitiveRoundTripPreservesValues() throws Exception {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(buffer)) {
      BinarySerialization.writeBoolean(out, true);
      BinarySerialization.writeByte(out, (byte) 7);
      BinarySerialization.writeShort(out, (short) 32000);
      BinarySerialization.writeInt(out, 123456789);
      BinarySerialization.writeLong(out, 9876543210L);
      BinarySerialization.writeFloat(out, 1.25f);
      BinarySerialization.writeDouble(out, 9.5d);
      BinarySerialization.writeChar(out, 'Z');
    }

    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer.toByteArray()))) {
      assertEquals(true, BinarySerialization.readBoolean(in));
      assertEquals((byte) 7, BinarySerialization.readByte(in));
      assertEquals((short) 32000, BinarySerialization.readShort(in));
      assertEquals(123456789, BinarySerialization.readInt(in));
      assertEquals(9876543210L, BinarySerialization.readLong(in));
      assertEquals(1.25f, BinarySerialization.readFloat(in));
      assertEquals(9.5d, BinarySerialization.readDouble(in));
      assertEquals('Z', BinarySerialization.readChar(in));
    }
  }

  @Test
  void structuredRoundTripPreservesValues() throws Exception {
    UUID id = UUID.randomUUID();
    byte[] raw = new byte[] {1, 2, 3, 4, 5};
    String text = "depchain";

    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(buffer)) {
      BinarySerialization.writeUuid(out, id);
      BinarySerialization.writeBytes(out, raw);
      BinarySerialization.writeString(out, text);
    }

    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer.toByteArray()))) {
      assertEquals(id, BinarySerialization.readUuid(in));
      assertArrayEquals(raw, BinarySerialization.readBytes(in));
      assertEquals(text, BinarySerialization.readString(in));
    }
  }

  @Test
  void readBytesRejectsNegativeLength() throws Exception {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(buffer)) {
      out.writeInt(-1);
    }

    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer.toByteArray()))) {
      assertThrows(IOException.class, () -> BinarySerialization.readBytes(in));
    }
  }

  @Test
  void readBytesRejectsLengthBiggerThanAvailableData() throws Exception {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(buffer)) {
      out.writeInt(8);
      out.write(new byte[] {10, 11});
    }

    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer.toByteArray()))) {
      assertThrows(IOException.class, () -> BinarySerialization.readBytes(in));
    }
  }
}
