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
import java.net.SocketTimeoutException;
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
  void sendRequestWaitsForMatchingConnectionAndSequence() throws Exception {
    InetAddress loopback = InetAddress.getLoopbackAddress();

    try (DatagramSocket replicaSocket = new DatagramSocket(0, loopback);
         FairLossLink clientTransport = FairLossLink.unbound(1_000, 4096)) {
      ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
        Future<?> worker =
            executor.submit(
                () -> {
                  try {
                    DatagramPacket requestPacket = new DatagramPacket(new byte[4096], 4096);
                    replicaSocket.receive(requestPacket);

                    Dpch request =
                        DpchCodec.fromBytes(
                            requestPacket.getData(), requestPacket.getOffset(), requestPacket.getLength());

                    Dpch mismatched =
                        Dpch.data(
                            request.connectionId() + 1,
                            request.sequenceNumber(),
                            "wrong".getBytes(StandardCharsets.UTF_8));
                    byte[] mismatchedBytes = DpchCodec.toBytes(mismatched);
                    replicaSocket.send(
                        new DatagramPacket(
                            mismatchedBytes,
                            mismatchedBytes.length,
                            requestPacket.getAddress(),
                            requestPacket.getPort()));

                    Dpch matched =
                        Dpch.data(
                            request.connectionId(),
                            request.sequenceNumber(),
                            "matched".getBytes(StandardCharsets.UTF_8));
                    byte[] matchedBytes = DpchCodec.toBytes(matched);
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

        Dpch outbound =
            Dpch.data(101, 7, "hello".getBytes(StandardCharsets.UTF_8));
        Dpch response =
            clientTransport.sendRequest(outbound, loopback, replicaSocket.getLocalPort());

        assertEquals("matched", new String(response.payload(), StandardCharsets.UTF_8));
        assertEquals(outbound.connectionId(), response.connectionId());
        assertEquals(outbound.sequenceNumber(), response.sequenceNumber());

        worker.get(2, TimeUnit.SECONDS);
      } finally {
        executor.shutdownNow();
      }
    }
  }

  @Test
  @Timeout(5)
  void sendRequestThrowsTimeoutWhenNoReplyArrives() throws Exception {
    InetAddress loopback = InetAddress.getLoopbackAddress();

    try (DatagramSocket blackhole = new DatagramSocket(0, loopback);
         FairLossLink clientTransport = FairLossLink.unbound(200, 4096)) {
      Dpch outbound =
          Dpch.data(50, 1, "no-reply".getBytes(StandardCharsets.UTF_8));
      SocketTimeoutException timeout =
          assertThrows(
              SocketTimeoutException.class,
              () -> clientTransport.sendRequest(outbound, loopback, blackhole.getLocalPort()));
      assertTrue(timeout.getMessage().contains("Timeout waiting for reply"));
    }
  }

  @Test
  @Timeout(5)
  void receiveRequestSkipsMalformedPacketAndReturnsNextValidOne() throws Exception {
    InetAddress loopback = InetAddress.getLoopbackAddress();
    int port = pickFreeUdpPort(loopback);

    try (FairLossLink serverTransport = FairLossLink.bind(loopback, port, 1_000, 4096);
         DatagramSocket sender = new DatagramSocket(0, loopback)) {
      byte[] malformed = new byte[] {0x01, 0x02, 0x03};
      sender.send(new DatagramPacket(malformed, malformed.length, loopback, port));

      Dpch valid = Dpch.data(77, 3, "valid".getBytes(StandardCharsets.UTF_8));
      byte[] validBytes = DpchCodec.toBytes(valid);
      sender.send(new DatagramPacket(validBytes, validBytes.length, loopback, port));

      InboundRequest inbound = serverTransport.receiveRequest();
      assertEquals(valid.connectionId(), inbound.packet().connectionId());
      assertEquals(valid.sequenceNumber(), inbound.packet().sequenceNumber());
      assertEquals(sender.getLocalPort(), inbound.senderPort());
      assertNotNull(inbound.senderIp());
    }
  }

  @Test
  void sendRequestFailsWhenPacketExceedsConfiguredMaxPacketSize() throws Exception {
    InetAddress loopback = InetAddress.getLoopbackAddress();

    try (FairLossLink clientTransport = FairLossLink.unbound(1_000, 64)) {
      Dpch oversized = Dpch.data(1, 1, new byte[8_192]);
      IOException error =
          assertThrows(
              IOException.class,
              () -> clientTransport.sendRequest(oversized, loopback, 9999));
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
              try (FairLossLink serverTransport = FairLossLink.bind(loopback, port, 2_000, 4096)) {
                serverReady.countDown();
                for (int i = 0; i < clientCount; i++) {
                  InboundRequest inbound = serverTransport.receiveRequest();
                  String text = new String(inbound.packet().payload(), StandardCharsets.UTF_8);
                  String key = inbound.packet().connectionId() + ":" + inbound.packet().sequenceNumber();
                  received.put(key, text);

                  Dpch response =
                      Dpch.data(
                          inbound.packet().connectionId(),
                          inbound.packet().sequenceNumber(),
                          ("ack:" + text).getBytes(StandardCharsets.UTF_8));
                  serverTransport.sendResponse(response, inbound.senderIp(), inbound.senderPort());
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
              try (FairLossLink clientTransport = FairLossLink.unbound(2_000, 4096)) {
                return clientTransport.sendRequest(outbound, loopback, port);
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

