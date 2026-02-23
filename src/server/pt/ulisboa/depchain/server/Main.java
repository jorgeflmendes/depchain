package pt.ulisboa.depchain.server;

import java.net.InetAddress;
import java.nio.file.Path;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import pt.ulisboa.depchain.shared.config.ConfigFile;
import pt.ulisboa.depchain.shared.udp.DatagramTransport;

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

    try (DatagramTransport transport = DatagramTransport.bind(
        bindAddress, replica.clientPort(), config.timeouts().retransmitMs())) {
      System.out.printf("Replica %s listening for client UDP requests on %s:%d (config: %s)%n", replica.id(),
          replica.host(), replica.clientPort(), configPath);

      while (true) {
        try {
          DatagramTransport.ReceivedDatagram datagram = transport.receive(config.network().maxPacketSize());
          String value = new String(datagram.payload(), StandardCharsets.UTF_8);
          String response = "Received " + value;
          transport.send(datagram.address(), datagram.port(), response.getBytes(StandardCharsets.UTF_8));
        } catch (SocketTimeoutException ignored) {
          // keep waiting for UDP packets
        } catch (Exception ignored) {
          // ignore malformed messages
        }
      }
    }
  }
}
