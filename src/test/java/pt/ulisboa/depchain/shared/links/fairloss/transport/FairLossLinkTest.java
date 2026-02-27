package pt.ulisboa.depchain.shared.links.fairloss.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import pt.ulisboa.depchain.shared.links.fairloss.codec.DpchCodec;
import pt.ulisboa.depchain.shared.links.fairloss.message.Dpch;

class FairLossLinkTest {
  @Test
  @Timeout(5)
  void sendAndReceivePacketBetweenClientAndServer() throws Exception {
    InetAddress loopback = InetAddress.getLoopbackAddress();
    int port = pickFreeUdpPort(loopback);

    try (FairLossLink serverTransport = FairLossLink.bind(loopback, port, 4096);
         FairLossLink clientTransport = FairLossLink.unbound(4096)) {
      Dpch outbound = Dpch.data(101, 7, "hello".getBytes(StandardCharsets.UTF_8));
      clientTransport.send(outbound, loopback, port);

      InboundRequest inbound = serverTransport.receive();
      assertEquals(outbound.connectionId(), inbound.packet().connectionId());
      assertEquals(outbound.sequenceNumber(), inbound.packet().sequenceNumber());
      assertEquals("hello", new String(inbound.packet().payload(), StandardCharsets.UTF_8));
      assertNotNull(inbound.senderIp());

      Dpch response =
          Dpch.data(
              inbound.packet().connectionId(),
              inbound.packet().sequenceNumber(),
              "matched".getBytes(StandardCharsets.UTF_8));
      serverTransport.send(response, inbound.senderIp(), inbound.senderPort());

      InboundRequest reply = clientTransport.receive();
      assertEquals("matched", new String(reply.packet().payload(), StandardCharsets.UTF_8));
      assertEquals(outbound.connectionId(), reply.packet().connectionId());
      assertEquals(outbound.sequenceNumber(), reply.packet().sequenceNumber());
    }
  }

  @Test
  @Timeout(5)
  void receiveSkipsMalformedPacketAndReturnsNextValidOne() throws Exception {
    InetAddress loopback = InetAddress.getLoopbackAddress();
    int port = pickFreeUdpPort(loopback);

    try (FairLossLink serverTransport = FairLossLink.bind(loopback, port, 4096);
         DatagramSocket sender = new DatagramSocket(0, loopback)) {
      byte[] malformed = new byte[] {0x01, 0x02, 0x03};
      sender.send(new DatagramPacket(malformed, malformed.length, loopback, port));

      Dpch valid = Dpch.data(77, 3, "valid".getBytes(StandardCharsets.UTF_8));
      byte[] validBytes = DpchCodec.toBytes(valid);
      sender.send(new DatagramPacket(validBytes, validBytes.length, loopback, port));

      InboundRequest inbound = serverTransport.receive();
      assertEquals(valid.connectionId(), inbound.packet().connectionId());
      assertEquals(valid.sequenceNumber(), inbound.packet().sequenceNumber());
      assertEquals(sender.getLocalPort(), inbound.senderPort());
      assertNotNull(inbound.senderIp());
    }
  }

  @Test
  void sendFailsWhenPacketExceedsConfiguredMaxPacketSize() throws Exception {
    InetAddress loopback = InetAddress.getLoopbackAddress();

    try (FairLossLink clientTransport = FairLossLink.unbound(64)) {
      Dpch oversized = Dpch.data(1, 1, new byte[8_192]);
      IOException error =
          assertThrows(
              IOException.class,
              () -> clientTransport.send(oversized, loopback, 9999));
      assertTrue(error.getMessage().contains("exceeds maxPacketSize"));
    }
  }

  @Test
  @Timeout(15)
  void multipleClientsCanExchangePacketsWithSingleServer() throws Exception {
    final int clientCount = 12;
    final InetAddress loopback = InetAddress.getLoopbackAddress();
    final int port = pickFreeUdpPort(loopback);
    final CountDownLatch serverReady = new CountDownLatch(1);
    final Map<String, String> received = new ConcurrentHashMap<>();

    ExecutorService serverExecutor = Executors.newSingleThreadExecutor();
    Future<?> serverWorker =
        serverExecutor.submit(
            () -> {
              try (FairLossLink serverTransport = FairLossLink.bind(loopback, port, 4096)) {
                serverReady.countDown();
                for (int i = 0; i < clientCount; i++) {
                  InboundRequest inbound = serverTransport.receive();
                  String text = new String(inbound.packet().payload(), StandardCharsets.UTF_8);
                  String key = inbound.packet().connectionId() + ":" + inbound.packet().sequenceNumber();
                  received.put(key, text);

                  Dpch response =
                      Dpch.data(
                          inbound.packet().connectionId(),
                          inbound.packet().sequenceNumber(),
                          ("ack:" + text).getBytes(StandardCharsets.UTF_8));
                  serverTransport.send(response, inbound.senderIp(), inbound.senderPort());
                }
              } catch (Exception exception) {
                throw new RuntimeException(exception);
              }
            });

    assertTrue(serverReady.await(3, TimeUnit.SECONDS), "Server did not start in time");

    ExecutorService clientExecutor = Executors.newFixedThreadPool(clientCount);
    try {
      List<Callable<Dpch>> tasks = new ArrayList<>();
      List<Dpch> outboundPackets = new ArrayList<>();

      for (int i = 0; i < clientCount; i++) {
        Dpch outbound =
            Dpch.data(1_000 + i, i, ("client-" + i).getBytes(StandardCharsets.UTF_8));
        outboundPackets.add(outbound);
        tasks.add(
            () -> {
              try (FairLossLink clientTransport = FairLossLink.unbound(4096)) {
                clientTransport.send(outbound, loopback, port);
                while (true) {
                  InboundRequest inbound = clientTransport.receive();
                  Dpch candidate = inbound.packet();
                  boolean sameConnectionId = candidate.connectionId() == outbound.connectionId();
                  boolean sameSequence = candidate.sequenceNumber() == outbound.sequenceNumber();
                  if (sameConnectionId && sameSequence) {
                    return candidate;
                  }
                }
              }
            });
      }

      List<Future<Dpch>> futures = clientExecutor.invokeAll(tasks);
      for (int i = 0; i < clientCount; i++) {
        Dpch outbound = outboundPackets.get(i);
        Dpch response = futures.get(i).get(5, TimeUnit.SECONDS);
        assertEquals("ack:client-" + i, new String(response.payload(), StandardCharsets.UTF_8));
        assertEquals(outbound.connectionId(), response.connectionId());
        assertEquals(outbound.sequenceNumber(), response.sequenceNumber());
      }

      serverWorker.get(5, TimeUnit.SECONDS);
      assertEquals(clientCount, received.size());
    } finally {
      clientExecutor.shutdownNow();
      serverExecutor.shutdownNow();
    }
  }

  private static int pickFreeUdpPort(InetAddress bindAddress) throws SocketException {
    try (DatagramSocket socket = new DatagramSocket(0, bindAddress)) {
      return socket.getLocalPort();
    }
  }
}

