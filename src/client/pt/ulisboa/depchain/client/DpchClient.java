package pt.ulisboa.depchain.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.keys.PrivateKeyLoader;
import pt.ulisboa.depchain.shared.keys.PublicKeyLoader;
import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.network.model.ClientRequest;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.SerializationUtil;

public final class DpchClient {
  private static final String RESET = "\u001B[0m";
  private static final String RED = "\u001B[31m";
  private static final String GREEN = "\u001B[32m";
  private static final String YELLOW = "\u001B[33m";

  private final ConfigParser.ReplicaSection targetReplicaConfig;
  private final long localSenderId;
  private final PrivateKey localStaticSKey;
  private final Map<Long, PublicKey> staticPKeys;

  public DpchClient(String targetReplicaId, String configPath) throws Exception {
    ConfigParser config = ConfigParser.load(Path.of(configPath));
    this.targetReplicaConfig = config.requireReplica(targetReplicaId);
    this.localSenderId = config.client().senderId();
    this.localStaticSKey = PrivateKeyLoader.loadClientPrivateKey(config);
    this.staticPKeys = PublicKeyLoader.loadStaticPublicKeys(config);
  }

  public void run() {
    try (Scanner scanner = new Scanner(System.in)) {
      runInputLoop(scanner);
    }

    System.out.println(GREEN + "Shutting down..." + RESET);
  }

  private void runInputLoop(Scanner scanner) {
    while (true) {
      System.out.println(YELLOW + "Enter a value to append or 'EXIT' to quit:" + RESET);
      String input = scanner.nextLine();

      if (input.equalsIgnoreCase("EXIT")) {
        break;
      }

      try {
        String response = sendAppendRequest(input);
        System.out.println(GREEN + "response = " + response + RESET);
      } catch (Exception exception) {
        System.err.println(RED + "Error appending value through the server " + targetReplicaConfig.id() + ": " + exception.getMessage() + RESET);
      }
    }
  }

  private String sendAppendRequest(String value) throws Exception {
    InetSocketAddress targetAddress = new InetSocketAddress(targetReplicaConfig.host(), targetReplicaConfig.clientPort());

    try (AuthenticatedLink transport = AuthenticatedLink.unbound(localSenderId, localStaticSKey, staticPKeys)) {
      return runRequestLoop(transport, value, targetAddress);
    }
  }

  private String runRequestLoop(AuthenticatedLink transport, String value, InetSocketAddress targetAddress) throws IOException, InterruptedException {
    long connectionId = ThreadLocalRandom.current().nextLong();
    try {
      ClientRequest request = createClientRequest(value);
      byte[] payload = SerializationUtil.encodeClientRequestBytes(request);
      transport.send(connectionId, payload, targetAddress);
    } catch (Exception exception) {
      throw new IOException("Could not sign client request", exception);
    }

    try {
      while (true) {
        InboundPacket inbound = receiveNextInbound(transport);
        if (inbound == null) {
          continue;
        }

        String reply = handleReply(inbound.packet(), connectionId);
        if (reply != null) {
          return reply;
        }
      }
    } finally {
      try {
        transport.closeConnection(connectionId, targetAddress);
      } catch (RuntimeException ignored) {
      }
    }
  }

  private InboundPacket receiveNextInbound(AuthenticatedLink transport) {
    try {
      return transport.receive();
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  private ClientRequest createClientRequest(String value) throws Exception {
    long requestId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    return ClientRequest.signed(localSenderId, requestId, value, localStaticSKey);
  }

  private String handleReply(Dpch inbound, long connectionId) {
    if (inbound.connectionId() != connectionId) {
      return null;
    }

    return SerializationUtil.decodeString(inbound.payload());
  }
}
