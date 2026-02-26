package pt.ulisboa.depchain.server;

import java.net.InetAddress;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import pt.ulisboa.depchain.shared.config.ConfigFile;
import pt.ulisboa.depchain.shared.links.fairloss.message.FairLossRequestMessage;
import pt.ulisboa.depchain.shared.links.fairloss.message.FairLossResponseMessage;
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

    // Use virtual threads to handle each request concurrently without blocking OS threads
    ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    try (workers;
        FairLossLink transport =
            FairLossLink.bind(
                bindAddress,
                replica.clientPort(),
                config.timeouts().retransmitMs(),
                config.network().maxPacketSize())) {
      System.out.printf("Replica %s listening for client UDP requests on %s:%d (config: %s)%n", replica.id(), replica.host(), replica.clientPort(), configPath);

      // Main server loop that receives requests and dispatches them to worker threads
      while (true) {
        InboundRequest request = transport.receiveRequest();
        workers.submit(() -> handleRequest(transport, request));
      }
    }
  }

  // Request handler that just echoes back the received value
  private static void handleRequest(FairLossLink transport, InboundRequest request) {
    try {
      // TODO: implement actual request handling 

      FairLossRequestMessage message = request.request();
      FairLossResponseMessage response = new FairLossResponseMessage(message.requestId(), true, "Received " + message.payload());
      transport.sendResponse(response, request.senderIp(), request.senderPort());
    } catch (Exception exception) {
    
      String sender = request.senderIp().getHostAddress() + ":" + request.senderPort();
      System.err.printf("Failed to handle request %s from %s: %s%n", request.request().requestId(), sender, exception.getMessage());
      exception.printStackTrace(System.err);
    }
  }
}
