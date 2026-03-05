package pt.ulisboa.depchain.shared.links.fairloss;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;
import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.dpch.DpchSerialization;
import pt.ulisboa.depchain.shared.network.dpch.DpchType;

class DpchSerializationTest {
  @Test
  void packetRoundTripPreservesFields() throws Exception {
    byte[] payload = "append-value".getBytes(StandardCharsets.UTF_8);
    Dpch packet = Dpch.from(ThreadLocalRandom.current().nextLong(), DpchType.DATA, 456, payload);

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
          Dpch.from(
              ThreadLocalRandom.current().nextLong(),
              DpchType.DATA,
              1,
              "d".getBytes(StandardCharsets.UTF_8)),
          Dpch.from(
              ThreadLocalRandom.current().nextLong(),
              DpchType.ACK,
              2,
              "a".getBytes(StandardCharsets.UTF_8)),
          Dpch.from(
              ThreadLocalRandom.current().nextLong(),
              DpchType.SYN,
              3,
              "s".getBytes(StandardCharsets.UTF_8)),
          Dpch.from(
              ThreadLocalRandom.current().nextLong(),
              DpchType.FIN,
              4,
              "f".getBytes(StandardCharsets.UTF_8))
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
  void packetRoundTripSupportsCombinedControlAckFlags() throws Exception {
    Dpch[] packets =
        new Dpch[] {
          Dpch.from(ThreadLocalRandom.current().nextLong(), DpchType.SYN, true, 11, new byte[0]),
          Dpch.from(ThreadLocalRandom.current().nextLong(), DpchType.FIN, true, 12, new byte[0])
        };

    for (Dpch packet : packets) {
      byte[] bytes = DpchSerialization.toBytes(packet);
      Dpch decoded = DpchSerialization.fromBytes(bytes, 0, bytes.length);

      assertEquals(packet.connectionId(), decoded.connectionId());
      assertEquals(packet.sequenceNumber(), decoded.sequenceNumber());
      assertEquals(packet.type(), decoded.type());
      assertTrue(decoded.hasType(packet.type()));
      assertTrue(decoded.hasType(DpchType.ACK));
    }
  }

  @Test
  void fromBytesSupportsOffsetAndLengthSlices() throws Exception {
    Dpch packet =
        Dpch.from(
            ThreadLocalRandom.current().nextLong(),
            DpchType.DATA,
            99,
            "slice".getBytes(StandardCharsets.UTF_8));
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
  void fromBytesRejectsWrongMagicVersionAndFlags() throws Exception {
    Dpch base =
        Dpch.from(
            ThreadLocalRandom.current().nextLong(),
            DpchType.DATA,
            2,
            "x".getBytes(StandardCharsets.UTF_8));
    byte[] bytes = DpchSerialization.toBytes(base);

    byte[] wrongMagic = Arrays.copyOf(bytes, bytes.length);
    wrongMagic[0] ^= 0x01;
    IOException magicError =
        assertThrows(IOException.class, () -> DpchSerialization.fromBytes(wrongMagic, 0, wrongMagic.length));
    assertTrue(magicError.getMessage().contains("Invalid message magic"));

    byte[] wrongVersion = Arrays.copyOf(bytes, bytes.length);
    wrongVersion[2] = 3;
    IOException versionError =
        assertThrows(IOException.class, () -> DpchSerialization.fromBytes(wrongVersion, 0, wrongVersion.length));
    assertTrue(versionError.getMessage().contains("Unsupported message version"));

    byte[] wrongFlags = Arrays.copyOf(bytes, bytes.length);
    wrongFlags[3] = (byte) 0b0010_0011; // ACK|DATA plus reserved bit set.
    IOException flagsError =
        assertThrows(IOException.class, () -> DpchSerialization.fromBytes(wrongFlags, 0, wrongFlags.length));
    assertTrue(flagsError.getMessage().contains("DPCH flags"));

    byte[] invalidReliableCombo = Arrays.copyOf(bytes, bytes.length);
    invalidReliableCombo[3] = (byte) 0b0000_0101; // DATA|SYN (more than one reliable semantic).
    IOException comboError =
        assertThrows(IOException.class, () -> DpchSerialization.fromBytes(invalidReliableCombo, 0, invalidReliableCombo.length));
    assertTrue(comboError.getMessage().contains("reliable flags"));

    byte[] reservedBitFourSet = Arrays.copyOf(bytes, bytes.length);
    reservedBitFourSet[3] = (byte) 0b0001_0010; // ACK + reserved bit 4 set.
    IOException reservedBitError =
        assertThrows(IOException.class, () -> DpchSerialization.fromBytes(reservedBitFourSet, 0, reservedBitFourSet.length));
    assertTrue(reservedBitError.getMessage().contains("reserved DPCH flags"));
  }

  @Test
  void fromBytesRejectsInvalidSlicesAndShortHeader() throws Exception {
    byte[] bytes = new byte[] {1, 2, 3};
    assertThrows(NullPointerException.class, () -> DpchSerialization.fromBytes(null, 0, 0));
    assertThrows(IllegalArgumentException.class, () -> DpchSerialization.fromBytes(bytes, -1, 1));
    assertThrows(IllegalArgumentException.class, () -> DpchSerialization.fromBytes(bytes, 0, -1));
    assertThrows(IllegalArgumentException.class, () -> DpchSerialization.fromBytes(bytes, 4, 0));
    assertThrows(IllegalArgumentException.class, () -> DpchSerialization.fromBytes(bytes, 1, 5));

    Dpch packet =
        Dpch.from(
            ThreadLocalRandom.current().nextLong(),
            DpchType.DATA,
            1,
            "ok".getBytes(StandardCharsets.UTF_8));
    byte[] encoded = DpchSerialization.toBytes(packet);
    IOException shortHeaderError =
        assertThrows(IOException.class, () -> DpchSerialization.fromBytes(encoded, 0, 13));
    assertTrue(shortHeaderError.getMessage().contains("shorter than header"));
  }

  @Test
  void toBytesRejectsNullValues() {
    assertThrows(NullPointerException.class, () -> DpchSerialization.toBytes(null));
  }
}
