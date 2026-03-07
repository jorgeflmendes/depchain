package pt.ulisboa.depchain.shared.messages;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;

import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.dpch.DpchType;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;

class InboundPacketTest {
  @Test
  void constructorPreservesFields() throws Exception {
    Dpch packet =
        Dpch.from(
            ThreadLocalRandom.current().nextLong(),
            DpchType.DATA,
            2,
            "payload".getBytes(StandardCharsets.UTF_8));
    InetSocketAddress sender = new InetSocketAddress(InetAddress.getLoopbackAddress(), 12000);
    InboundPacket inbound = new InboundPacket(sender, packet);

    assertEquals(sender, inbound.sender());
    assertEquals(packet, inbound.packet());
  }

  @Test
  void constructorRejectsInvalidArguments() {
    Dpch packet =
        Dpch.from(
            ThreadLocalRandom.current().nextLong(),
            DpchType.DATA,
            2,
            "payload".getBytes(StandardCharsets.UTF_8));
    InetSocketAddress sender = new InetSocketAddress(InetAddress.getLoopbackAddress(), 12000);

    assertThrows(NullPointerException.class, () -> new InboundPacket(null, packet));
    assertThrows(NullPointerException.class, () -> new InboundPacket(sender, null));
    assertThrows(
        IllegalArgumentException.class,
        () -> new InboundPacket(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), packet));
  }
}

