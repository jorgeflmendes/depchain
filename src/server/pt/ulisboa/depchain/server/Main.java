package pt.ulisboa.depchain.server;

import java.net.InetAddress;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import pt.ulisboa.depchain.shared.config.ConfigFile;
import pt.ulisboa.depchain.shared.udp.UdpRequestResponseTransport;
import pt.ulisboa.depchain.shared.udp.messages.MessageRequest;
import pt.ulisboa.depchain.shared.udp.messages.MessageResponse;

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

    ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor(); // use virtual threads to handle each request concurrently without blocking OS threads
    try (workers; UdpRequestResponseTransport transport = UdpRequestResponseTransport.bind(bindAddress, replica.clientPort(), config.timeouts().retransmitMs(), config.network().maxPacketSize())) {
      System.out.printf("Replica %s listening for client UDP requests on %s:%d (config: %s)%n", replica.id(), replica.host(), replica.clientPort(), configPath);

      while (true) {
        MessageRequest request = transport.receiveRequest();
        workers.submit(() -> handleRequest(transport, request));
      }
    }
  }

  // request handler that just echoes back the received value
  private static void handleRequest(UdpRequestResponseTransport transport, MessageRequest request) {
    try {
      // TODO: implement actual request handling 

      String value = String.valueOf(request.payload());
      MessageResponse response = new MessageResponse(request.requestId(), true, "Received " + value);
      transport.sendResponse(response, request.senderIp(), request.senderPort());
    } catch (Exception ignored) {
      // Keep serving other clients if one response fails.
    }
  }
}
