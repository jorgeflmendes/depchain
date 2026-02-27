package pt.ulisboa.depchain.server;

import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import pt.ulisboa.depchain.shared.config.ConfigFile;
import pt.ulisboa.depchain.shared.links.fairloss.message.Dpch;
import pt.ulisboa.depchain.shared.links.fairloss.transport.FairLossLink;
import pt.ulisboa.depchain.shared.links.fairloss.transport.InboundRequest;

public final class Main {
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: Main <serverId> <configPath>");
      System.exit(1);
    }

    String serverId = args[0];
    String configPath = args[1];

    ConfigFile config = ConfigFile.load(Path.of(configPath));
    ConfigFile.ReplicaSection replica = config.requireReplica(serverId);
    InetAddress bindAddress = InetAddress.getByName(replica.host());

    // Use virtual threads to handle each request concurrently without blocking OS threads.
    ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    try (workers; FairLossLink transport = FairLossLink.bind(bindAddress, replica.clientPort(), config.network().maxPacketSize())) {
      System.out.printf("Replica %s listening for client UDP requests on %s:%d (config: %s)%n", replica.id(), replica.host(), replica.clientPort(), configPath);

      // Main server loop that receives requests and dispatches them to worker threads.
      while (true) {
        try {
          // Receive one request packet from the UDP fair-loss transport.
          InboundRequest request = transport.receive();
          workers.submit(() -> handleRequest(transport, request));
        } catch (Exception exception) {
          System.out.printf("Packet exchange error while receiving = %s%n", exception.getMessage());
          exception.printStackTrace(System.out);
        }
      }
    }
  }

  // Request handler that just echoes back the received value
  private static void handleRequest(FairLossLink transport, InboundRequest request) {
    try {
      Dpch inbound = request.packet();
      String payloadText = new String(inbound.payload(), StandardCharsets.UTF_8);
      byte[] responsePayload = ("Received " + payloadText).getBytes(StandardCharsets.UTF_8);
      Dpch response = Dpch.data(inbound.connectionId(), inbound.sequenceNumber(), responsePayload);

      // Send the response packet back to the original sender (IP + port).
      transport.send(response, request.senderIp(), request.senderPort());
    } catch (Exception exception) {
    
      String sender = request.senderIp().getHostAddress() + ":" + request.senderPort();
      System.out.printf(
          "Packet exchange error while handling conn=%d seq=%d from %s = %s%n",
          request.packet().connectionId(),
          request.packet().sequenceNumber(),
          sender,
          exception.getMessage());
      exception.printStackTrace(System.out);
    }
  }
}
