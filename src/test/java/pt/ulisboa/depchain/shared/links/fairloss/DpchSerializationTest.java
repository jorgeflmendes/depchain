package pt.ulisboa.depchain.shared.links.fairloss;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.dpch.DpchSerialization;

class DpchSerializationTest {
  @Test
  void packetRoundTripPreservesFields() throws Exception {
    byte[] payload = "append-value".getBytes(StandardCharsets.UTF_8);
    Dpch packet = Dpch.data(123, 456, payload);

    byte[] bytes = DpchSerialization.toBytes(packet);
    Object decoded = DpchSerialization.fromBytes(bytes, 0, bytes.length);
    Dpch decodedPacket = assertInstanceOf(Dpch.class, decoded);

    assertEquals(packet.connectionId(), decodedPacket.connectionId());
    assertEquals(packet.type(), decodedPacket.type());
    assertEquals(packet.sequenceNumber(), decodedPacket.sequenceNumber());
    assertTrue(Arrays.equals(payload, decodedPacket.payload()));
  }

  @Test
  void packetRoundTripSupportsAllDefinedTypes() throws Exception {
    Dpch[] packets =
        new Dpch[] {
          Dpch.data(10, 1, "d".getBytes(StandardCharsets.UTF_8)),
          Dpch.ack(10, 2, "a".getBytes(StandardCharsets.UTF_8)),
          Dpch.syn(10, 3, "s".getBytes(StandardCharsets.UTF_8)),
          Dpch.fin(10, 4, "f".getBytes(StandardCharsets.UTF_8))
        };

    for (Dpch packet : packets) {
      byte[] bytes = DpchSerialization.toBytes(packet);
      Dpch decoded = DpchSerialization.fromBytes(bytes, 0, bytes.length);

      assertEquals(packet.connectionId(), decoded.connectionId());
      assertEquals(packet.type(), decoded.type());
      assertEquals(packet.sequenceNumber(), decoded.sequenceNumber());
      assertTrue(Arrays.equals(packet.payload(), decoded.payload()));
    }
  }

  @Test
  void fromBytesSupportsOffsetAndLengthSlices() throws Exception {
    Dpch packet = Dpch.data(10, 99, "slice".getBytes(StandardCharsets.UTF_8));
    byte[] encoded = DpchSerialization.toBytes(packet);
    byte[] wrapped = new byte[encoded.length + 16];
    Arrays.fill(wrapped, (byte) 0x33);
    System.arraycopy(encoded, 0, wrapped, 7, encoded.length);

    Dpch decoded = DpchSerialization.fromBytes(wrapped, 7, encoded.length);
    assertEquals(packet.connectionId(), decoded.connectionId());
    assertEquals(packet.sequenceNumber(), decoded.sequenceNumber());
    assertTrue(Arrays.equals(packet.payload(), decoded.payload()));
  }

  @Test
  void fromBytesRejectsWrongMagicVersionAndType() throws Exception {
    Dpch base = Dpch.data(1, 2, "x".getBytes(StandardCharsets.UTF_8));
    byte[] bytes = DpchSerialization.toBytes(base);

    byte[] wrongMagic = Arrays.copyOf(bytes, bytes.length);
    wrongMagic[0] ^= 0x01;
    IOException magicError =
        assertThrows(IOException.class, () -> DpchSerialization.fromBytes(wrongMagic, 0, wrongMagic.length));
    assertTrue(magicError.getMessage().contains("Invalid message magic"));

    byte[] wrongVersion = Arrays.copyOf(bytes, bytes.length);
    wrongVersion[4] = 2;
    IOException versionError =
        assertThrows(IOException.class, () -> DpchSerialization.fromBytes(wrongVersion, 0, wrongVersion.length));
    assertTrue(versionError.getMessage().contains("Unsupported message version"));

    byte[] wrongType = Arrays.copyOf(bytes, bytes.length);
    wrongType[9] = 99;
    IOException typeError =
        assertThrows(IOException.class, () -> DpchSerialization.fromBytes(wrongType, 0, wrongType.length));
    assertTrue(typeError.getMessage().contains("Unsupported DPCH packet type"));
  }

  @Test
  void fromBytesRejectsInvalidSlicesAndPayloadLengths() throws Exception {
    byte[] bytes = new byte[] {1, 2, 3};
    assertThrows(NullPointerException.class, () -> DpchSerialization.fromBytes(null, 0, 0));
    assertThrows(IllegalArgumentException.class, () -> DpchSerialization.fromBytes(bytes, -1, 1));
    assertThrows(IllegalArgumentException.class, () -> DpchSerialization.fromBytes(bytes, 0, -1));
    assertThrows(IllegalArgumentException.class, () -> DpchSerialization.fromBytes(bytes, 4, 0));
    assertThrows(IllegalArgumentException.class, () -> DpchSerialization.fromBytes(bytes, 1, 5));

    Dpch packet = Dpch.data(1, 1, "ok".getBytes(StandardCharsets.UTF_8));
    byte[] encoded = DpchSerialization.toBytes(packet);
    byte[] wrongLength = Arrays.copyOf(encoded, encoded.length);
    wrongLength[14] = 0;
    wrongLength[15] = 8;
    IOException lengthError =
        assertThrows(IOException.class, () -> DpchSerialization.fromBytes(wrongLength, 0, wrongLength.length));
    assertTrue(lengthError.getMessage().contains("Invalid payload length"));
  }

  @Test
  void fromBytesRejectsTrailingBytes() throws Exception {
    Dpch packet = Dpch.data(4, 5, "x".getBytes(StandardCharsets.UTF_8));
    byte[] encoded = DpchSerialization.toBytes(packet);
    byte[] withTrailing = Arrays.copyOf(encoded, encoded.length + 2);
    withTrailing[encoded.length] = 11;
    withTrailing[encoded.length + 1] = 22;

    IOException error =
        assertThrows(IOException.class, () -> DpchSerialization.fromBytes(withTrailing, 0, withTrailing.length));
    assertTrue(error.getMessage().contains("Trailing bytes"));
  }

  @Test
  void toBytesRejectsNullValues() {
    assertThrows(NullPointerException.class, () -> DpchSerialization.toBytes(null));
  }
}
