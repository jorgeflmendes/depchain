package pt.ulisboa.depchain.shared.messages;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.model.InboundMessage;

class InboundMessageTest {
  @Test
  void constructorPreservesFields() throws Exception {
    Dpch packet = Dpch.data(1, 2, "payload".getBytes(StandardCharsets.UTF_8));
    InetAddress senderIp = InetAddress.getLoopbackAddress();
    InboundMessage inbound = new InboundMessage(packet, senderIp, 12000);

    assertEquals(packet, inbound.packet());
    assertEquals(senderIp, inbound.senderIp());
    assertEquals(12000, inbound.senderPort());
  }

  @Test
  void constructorRejectsInvalidArguments() {
    Dpch packet = Dpch.data(1, 2, "payload".getBytes(StandardCharsets.UTF_8));
    InetAddress senderIp = InetAddress.getLoopbackAddress();

    assertThrows(NullPointerException.class, () -> new InboundMessage(null, senderIp, 12000));
    assertThrows(NullPointerException.class, () -> new InboundMessage(packet, null, 12000));
    assertThrows(IllegalArgumentException.class, () -> new InboundMessage(packet, senderIp, 0));
    assertThrows(IllegalArgumentException.class, () -> new InboundMessage(packet, senderIp, 65536));
  }
}
