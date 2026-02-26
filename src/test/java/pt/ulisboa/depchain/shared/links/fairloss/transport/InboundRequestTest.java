package pt.ulisboa.depchain.shared.links.fairloss.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import pt.ulisboa.depchain.shared.links.fairloss.message.Dpch;

class InboundRequestTest {
  @Test
  void constructorPreservesFields() throws Exception {
    Dpch packet = Dpch.data(1, 2, "payload".getBytes(StandardCharsets.UTF_8));
    InetAddress senderIp = InetAddress.getLoopbackAddress();
    InboundRequest inbound = new InboundRequest(packet, senderIp, 12000);

    assertEquals(packet, inbound.packet());
    assertEquals(senderIp, inbound.senderIp());
    assertEquals(12000, inbound.senderPort());
  }

  @Test
  void constructorRejectsInvalidArguments() {
    Dpch packet = Dpch.data(1, 2, "payload".getBytes(StandardCharsets.UTF_8));
    InetAddress senderIp = InetAddress.getLoopbackAddress();

    assertThrows(NullPointerException.class, () -> new InboundRequest(null, senderIp, 12000));
    assertThrows(NullPointerException.class, () -> new InboundRequest(packet, null, 12000));
    assertThrows(IllegalArgumentException.class, () -> new InboundRequest(packet, senderIp, 0));
    assertThrows(IllegalArgumentException.class, () -> new InboundRequest(packet, senderIp, 65536));
  }
}

