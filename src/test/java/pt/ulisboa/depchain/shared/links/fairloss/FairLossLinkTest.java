package pt.ulisboa.depchain.shared.links.fairloss;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
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
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.dpch.DpchSerialization;
import pt.ulisboa.depchain.shared.network.dpch.DpchType;
import pt.ulisboa.depchain.shared.network.links.fairloss.FairLossLink;
import pt.ulisboa.depchain.shared.network.links.fairloss.InboundBytes;

class FairLossLinkTest {
  @Test
  @Timeout(5)
  void sendAndReceivePacketBetweenClientAndServer() throws Exception {
    InetAddress loopback = InetAddress.getLoopbackAddress();
    int port = pickFreeUdpPort(loopback);
    InetSocketAddress serverEndpoint = new InetSocketAddress(loopback, port);

    try (FairLossLink serverTransport = FairLossLink.bind(serverEndpoint, 4096);
         FairLossLink clientTransport = FairLossLink.unbound(4096)) {
      Dpch outbound =
          Dpch.from(
              ThreadLocalRandom.current().nextLong(),
              DpchType.DATA,
              7,
              "hello".getBytes(StandardCharsets.UTF_8));
      clientTransport.send(DpchSerialization.toBytes(outbound), serverEndpoint);

      InboundBytes inboundRaw = serverTransport.receive();
      Dpch inbound = DpchSerialization.fromBytes(inboundRaw.payload(), 0, inboundRaw.payload().length);
      assertEquals(outbound.connectionId(), inbound.connectionId());
      assertEquals(outbound.sequenceNumber(), inbound.sequenceNumber());
      assertEquals("hello", new String(inbound.payload(), StandardCharsets.UTF_8));
      assertNotNull(inboundRaw.sender());

      Dpch response =
          Dpch.from(
              inbound.connectionId(),
              DpchType.DATA,
              inbound.sequenceNumber(),
              "matched".getBytes(StandardCharsets.UTF_8));
      serverTransport.send(DpchSerialization.toBytes(response), inboundRaw.sender());

      InboundBytes replyRaw = clientTransport.receive();
      Dpch reply = DpchSerialization.fromBytes(replyRaw.payload(), 0, replyRaw.payload().length);
      assertEquals("matched", new String(reply.payload(), StandardCharsets.UTF_8));
      assertEquals(outbound.connectionId(), reply.connectionId());
      assertEquals(outbound.sequenceNumber(), reply.sequenceNumber());
    }
  }

  @Test
  @Timeout(5)
  void receiveReturnsRawMalformedPacket() throws Exception {
    InetAddress loopback = InetAddress.getLoopbackAddress();
    int port = pickFreeUdpPort(loopback);
    InetSocketAddress serverEndpoint = new InetSocketAddress(loopback, port);

    try (FairLossLink serverTransport = FairLossLink.bind(serverEndpoint, 4096);
         DatagramSocket sender = new DatagramSocket(0, loopback)) {
      byte[] malformed = new byte[] {0x01, 0x02, 0x03};
      sender.send(new DatagramPacket(malformed, malformed.length, loopback, port));
      InboundBytes inbound = serverTransport.receive();
      assertEquals(malformed.length, inbound.payload().length);
    }
  }

  @Test
  void sendFailsWhenPacketExceedsConfiguredMaxPacketSize() throws Exception {
    InetAddress loopback = InetAddress.getLoopbackAddress();

    try (FairLossLink clientTransport = FairLossLink.unbound(64)) {
      byte[] oversized = new byte[8_192];
      IOException error =
          assertThrows(
              IOException.class,
              () -> clientTransport.send(oversized, new InetSocketAddress(loopback, 9999)));
      assertTrue(error.getMessage().contains("exceeds maxPacketSize"));
    }
  }

  @Test
  void sendRejectsInvalidRemotePort() throws Exception {
    InetAddress loopback = InetAddress.getLoopbackAddress();

    try (FairLossLink clientTransport = FairLossLink.unbound(64)) {
      byte[] payload = new byte[] {1, 2, 3};
      assertThrows(
          IllegalArgumentException.class,
          () -> clientTransport.send(payload, new InetSocketAddress(loopback, 0)));
      assertThrows(
          IllegalArgumentException.class,
          () -> clientTransport.send(payload, new InetSocketAddress(loopback, 65536)));
    }
  }

  @Test
  @Timeout(15)
  void multipleClientsCanExchangePacketsWithSingleServer() throws Exception {
    final int clientCount = 12;
    final InetAddress loopback = InetAddress.getLoopbackAddress();
    final int port = pickFreeUdpPort(loopback);
    final InetSocketAddress serverEndpoint = new InetSocketAddress(loopback, port);
    final CountDownLatch serverReady = new CountDownLatch(1);
    final Map<String, String> received = new ConcurrentHashMap<>();

    ExecutorService serverExecutor = Executors.newSingleThreadExecutor();
    Future<?> serverWorker =
        serverExecutor.submit(
            () -> {
              try (FairLossLink serverTransport = FairLossLink.bind(serverEndpoint, 4096)) {
                serverReady.countDown();
                for (int i = 0; i < clientCount; i++) {
                  InboundBytes inboundRaw = serverTransport.receive();
                  Dpch inbound = DpchSerialization.fromBytes(inboundRaw.payload(), 0, inboundRaw.payload().length);
                  String text = new String(inbound.payload(), StandardCharsets.UTF_8);
                  String key = inbound.connectionId() + ":" + inbound.sequenceNumber();
                  received.put(key, text);

                  Dpch response =
                      Dpch.from(
                          inbound.connectionId(),
                          DpchType.DATA,
                          inbound.sequenceNumber(),
                          ("ack:" + text).getBytes(StandardCharsets.UTF_8));
                  serverTransport.send(DpchSerialization.toBytes(response), inboundRaw.sender());
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
            Dpch.from(
                ThreadLocalRandom.current().nextLong(),
                DpchType.DATA,
                i,
                ("client-" + i).getBytes(StandardCharsets.UTF_8));
        outboundPackets.add(outbound);
        tasks.add(
            () -> {
              try (FairLossLink clientTransport = FairLossLink.unbound(4096)) {
                clientTransport.send(DpchSerialization.toBytes(outbound), serverEndpoint);
                while (true) {
                  InboundBytes inboundRaw = clientTransport.receive();
                  Dpch candidate = DpchSerialization.fromBytes(inboundRaw.payload(), 0, inboundRaw.payload().length);
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
