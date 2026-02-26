package pt.ulisboa.depchain.shared.links.fairloss.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import pt.ulisboa.depchain.shared.links.fairloss.codec.FairLossPacketCodec;
import pt.ulisboa.depchain.shared.links.fairloss.message.FairLossRequestMessage;
import pt.ulisboa.depchain.shared.links.fairloss.message.FairLossResponseMessage;

class FairLossLinkTest {
  @Test
  @Timeout(5)
  void sendRequestWaitsForMatchingRequestId() throws Exception {
    InetAddress loopback = InetAddress.getLoopbackAddress();

    try (DatagramSocket replicaSocket = new DatagramSocket(0, loopback);
         FairLossLink clientTransport = FairLossLink.unbound(1_000, 4096)) {
      ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
        Future<?> worker = executor.submit(() -> {
          try {
            DatagramPacket requestPacket = new DatagramPacket(new byte[4096], 4096);
            replicaSocket.receive(requestPacket);

            FairLossRequestMessage request =
                (FairLossRequestMessage)
                    FairLossPacketCodec.fromBytes(
                        requestPacket.getData(), requestPacket.getOffset(), requestPacket.getLength());

            // Send a response for a different requestId first; client should ignore it.
            FairLossResponseMessage mismatched =
                new FairLossResponseMessage(UUID.randomUUID(), true, "wrong");
            byte[] mismatchedBytes = FairLossPacketCodec.toBytes(mismatched);
            replicaSocket.send(
                new DatagramPacket(
                    mismatchedBytes,
                    mismatchedBytes.length,
                    requestPacket.getAddress(),
                    requestPacket.getPort()));

            FairLossResponseMessage matched =
                new FairLossResponseMessage(request.requestId(), true, "matched");
            byte[] matchedBytes = FairLossPacketCodec.toBytes(matched);
            replicaSocket.send(
                new DatagramPacket(
                    matchedBytes,
                    matchedBytes.length,
                    requestPacket.getAddress(),
                    requestPacket.getPort()));
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

        FairLossRequestMessage outbound = new FairLossRequestMessage(UUID.randomUUID(), "hello");
        FairLossResponseMessage response =
            clientTransport.sendRequest(outbound, loopback, replicaSocket.getLocalPort());

        assertTrue(response.success());
        assertEquals("matched", response.payload());
        assertEquals(outbound.requestId(), response.requestId());

        worker.get(2, TimeUnit.SECONDS);
      } finally {
        executor.shutdownNow();
      }
    }
  }
}
