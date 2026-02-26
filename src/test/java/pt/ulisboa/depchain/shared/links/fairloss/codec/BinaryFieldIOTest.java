package pt.ulisboa.depchain.shared.links.fairloss.codec;

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

class BinaryFieldIOTest {
  @Test
  void primitiveRoundTripPreservesValues() throws Exception {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(buffer)) {
      BinaryFieldIO.writeBoolean(out, true);
      BinaryFieldIO.writeByte(out, (byte) 7);
      BinaryFieldIO.writeShort(out, (short) 32000);
      BinaryFieldIO.writeInt(out, 123456789);
      BinaryFieldIO.writeLong(out, 9876543210L);
      BinaryFieldIO.writeFloat(out, 1.25f);
      BinaryFieldIO.writeDouble(out, 9.5d);
      BinaryFieldIO.writeChar(out, 'Z');
    }

    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer.toByteArray()))) {
      assertEquals(true, BinaryFieldIO.readBoolean(in));
      assertEquals((byte) 7, BinaryFieldIO.readByte(in));
      assertEquals((short) 32000, BinaryFieldIO.readShort(in));
      assertEquals(123456789, BinaryFieldIO.readInt(in));
      assertEquals(9876543210L, BinaryFieldIO.readLong(in));
      assertEquals(1.25f, BinaryFieldIO.readFloat(in));
      assertEquals(9.5d, BinaryFieldIO.readDouble(in));
      assertEquals('Z', BinaryFieldIO.readChar(in));
    }
  }

  @Test
  void structuredRoundTripPreservesValues() throws Exception {
    UUID id = UUID.randomUUID();
    byte[] raw = new byte[] {1, 2, 3, 4, 5};
    String text = "depchain";

    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(buffer)) {
      BinaryFieldIO.writeUuid(out, id);
      BinaryFieldIO.writeBytes(out, raw);
      BinaryFieldIO.writeString(out, text);
    }

    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer.toByteArray()))) {
      assertEquals(id, BinaryFieldIO.readUuid(in));
      assertArrayEquals(raw, BinaryFieldIO.readBytes(in));
      assertEquals(text, BinaryFieldIO.readString(in));
    }
  }

  @Test
  void readBytesRejectsNegativeLength() throws Exception {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(buffer)) {
      out.writeInt(-1);
    }

    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer.toByteArray()))) {
      assertThrows(IOException.class, () -> BinaryFieldIO.readBytes(in));
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
      assertThrows(IOException.class, () -> BinaryFieldIO.readBytes(in));
    }
  }
}
