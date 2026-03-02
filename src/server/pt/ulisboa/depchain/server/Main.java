package pt.ulisboa.depchain.server;

import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.links.perfect.PerfectLink;
import pt.ulisboa.depchain.shared.network.model.InboundMessage;

public final class Main {
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: Main <serverId> <configPath>");
      System.exit(1);
    }

    String serverId = args[0];
    String configPath = args[1];

    // Load the server configuration from the specified file path.
    ConfigParser config = ConfigParser.load(Path.of(configPath));
    ConfigParser.ReplicaSection replicaConfig = config.requireReplica(serverId);
    InetAddress bindAddress = InetAddress.getByName(replicaConfig.host());
    ConfigParser.StubbornSection stubbornConfig = config.stubborn();
    ConfigParser.PerfectSection perfectConfig = config.perfect();

    // Use virtual threads to handle each request concurrently without blocking OS threads.
    ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();

    // TODO: wtf do we need all these parameters?
    try (workers; PerfectLink perfectLink = PerfectLink.bind(bindAddress, replicaConfig.clientPort(), config.network().maxPacketSize(), stubbornConfig.baseDelayMs(), stubbornConfig.maxDelayMs(), stubbornConfig.jitterRatio(), stubbornConfig.maxPending(), stubbornConfig.heapCompactMinSize(), stubbornConfig.maxRetryAttempts(), stubbornConfig.maxTrackedLifetimeMs(), perfectConfig.maxWindowSize(), perfectConfig.maxStreamStates(), perfectConfig.streamIdleTtlMs())) {
      System.out.printf("Replica %s listening for client UDP requests on %s:%d (config: %s)%n", replicaConfig.id(), replicaConfig.host(), replicaConfig.clientPort(), configPath);

      while (true) {
        // Receive one request packet from the perfect-link transport.
        InboundMessage request = perfectLink.receive();
        workers.submit(() -> handleRequest(perfectLink, request.packet(), request.senderIp(), request.senderPort()));
      }
    }
  }

  // Request handler that just echoes back the received value
  private static void handleRequest(PerfectLink transport, Dpch inbound, InetAddress senderIp, int senderPort) {
    try {
      String payloadText = new String(inbound.payload(), StandardCharsets.UTF_8);
      byte[] responsePayload = ("Received " + payloadText).getBytes(StandardCharsets.UTF_8);
      transport.sendReliable(inbound.connectionId(), responsePayload, senderIp, senderPort);
    } catch (Exception exception) {
      String sender = senderIp.getHostAddress() + ":" + senderPort;
      System.out.printf("Packet exchange error while handling conn=%d seq=%d from %s = %s%n", inbound.connectionId(), inbound.sequenceNumber(), sender, exception.getMessage());
    }
  }
}
