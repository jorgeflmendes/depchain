package pt.ulisboa.depchain.server;

import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import pt.ulisboa.depchain.shared.config.ConfigFile;
import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.links.stubborn.StubbornLink;
import pt.ulisboa.depchain.shared.network.messages.InboundMessage;

public final class Main {
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: Main <serverId> <configPath>");
      System.exit(1);
    }

    String serverId = args[0];
    String configPath = args[1];

    // Load the server configuration from the specified file path.
    ConfigFile config = ConfigFile.load(Path.of(configPath));
    ConfigFile.ReplicaSection replicaConfig = config.requireReplica(serverId);
    InetAddress bindAddress = InetAddress.getByName(replicaConfig.host());
    ConfigFile.StubbornSection stubbornConfig = config.stubborn();

    // Use virtual threads to handle each request concurrently without blocking OS threads.
    ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    try (workers; StubbornLink transport = StubbornLink.bind(bindAddress, replicaConfig.clientPort(), config.network().maxPacketSize(), stubbornConfig.baseDelayMs(), stubbornConfig.maxDelayMs(), stubbornConfig.jitterRatio(), stubbornConfig.maxPending(), stubbornConfig.heapCompactMinSize())) {
      System.out.printf("Replica %s listening for client UDP requests on %s:%d (config: %s)%n", replicaConfig.id(), replicaConfig.host(), replicaConfig.clientPort(), configPath);

      // Main server loop that receives requests and dispatches them to worker threads.
      while (true) {
        // Receive one request packet from the UDP stubbornConfig transport.
        InboundMessage request = transport.receive();
        workers.submit(() -> handleRequest(transport, request.packet(), request.senderIp(), request.senderPort()));
      }
    }
  }

  // Request handler that just echoes back the received value
  private static void handleRequest(StubbornLink transport, Dpch inbound, InetAddress senderIp, int senderPort) {
    try {
      String payloadText = new String(inbound.payload(), StandardCharsets.UTF_8);
      byte[] responsePayload = ("Received " + payloadText).getBytes(StandardCharsets.UTF_8);
      Dpch response = Dpch.data(inbound.connectionId(), inbound.sequenceNumber(), responsePayload);

      // Send one response packet back to the original sender (IP + port).
      transport.sendOnce(response, senderIp, senderPort);
    } catch (Exception exception) {
      String sender = senderIp.getHostAddress() + ":" + senderPort;
      System.out.printf("Packet exchange error while handling conn=%d seq=%d from %s = %s%n", inbound.connectionId(), inbound.sequenceNumber(), sender, exception.getMessage());
    }
  }
}
