package pt.ulisboa.depchain.shared.links.fairloss.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import pt.ulisboa.depchain.shared.links.fairloss.message.FairLossRequestMessage;
import pt.ulisboa.depchain.shared.links.fairloss.message.FairLossResponseMessage;

class FairLossPacketCodecTest {
  @Test
  void requestRoundTripPreservesFields() throws Exception {
    FairLossRequestMessage request = new FairLossRequestMessage(UUID.randomUUID(), "append-value");

    byte[] bytes = FairLossPacketCodec.toBytes(request);
    Object decoded = FairLossPacketCodec.fromBytes(bytes, 0, bytes.length);

    FairLossRequestMessage decodedRequest = assertInstanceOf(FairLossRequestMessage.class, decoded);
    assertEquals(request.requestId(), decodedRequest.requestId());
    assertEquals(request.payload(), decodedRequest.payload());
  }

  @Test
  void responseRoundTripPreservesFields() throws Exception {
    FairLossResponseMessage response = new FairLossResponseMessage(UUID.randomUUID(), true, "ok");

    byte[] bytes = FairLossPacketCodec.toBytes(response);
    Object decoded = FairLossPacketCodec.fromBytes(bytes, 0, bytes.length);

    FairLossResponseMessage decodedResponse =
        assertInstanceOf(FairLossResponseMessage.class, decoded);
    assertEquals(response.requestId(), decodedResponse.requestId());
    assertEquals(response.success(), decodedResponse.success());
    assertEquals(response.payload(), decodedResponse.payload());
  }

  @Test
  void fromBytesRejectsInvalidPackets() {
    byte[] invalid = new byte[] {0, 1, 2, 3, 4};

    assertThrows(IOException.class, () -> FairLossPacketCodec.fromBytes(invalid, 0, invalid.length));
  }
}
