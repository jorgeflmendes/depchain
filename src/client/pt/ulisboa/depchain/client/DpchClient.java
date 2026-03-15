package pt.ulisboa.depchain.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import pt.ulisboa.depchain.proto.AppendRequest;
import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.ClientResponse;
import pt.ulisboa.depchain.proto.DpchPacket;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.keys.PrivateKeyLoader;
import pt.ulisboa.depchain.shared.keys.PublicKeyLoader;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.ClientRequestPayloadUtil;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;
import pt.ulisboa.depchain.shared.utils.ProtoValidationUtil;
import pt.ulisboa.depchain.shared.utils.TimeUtil;

public final class DpchClient {
  private static final Logger logger = LoggerFactory.getLogger(DpchClient.class);

  private final ConfigParser.ReplicaSection targetReplicaConfig;
  private final long localSenderId;
  private final PrivateKey localStaticSKey;
  private final Map<Long, PublicKey> staticPKeys;
  private final long requestTimeoutMs;

  public DpchClient(String targetReplicaId, String configPath) throws Exception {
    ConfigParser config = ConfigParser.load(Path.of(configPath));
    this.targetReplicaConfig = config.requireReplicaById(targetReplicaId);
    this.localSenderId = config.client().senderId();
    this.localStaticSKey = PrivateKeyLoader.loadClientPrivateKey(config);
    this.staticPKeys = PublicKeyLoader.loadStaticPublicKeys(config);
    this.requestTimeoutMs = config.client().requestTimeoutMs();
  }

  public void run() {
    try (Scanner scanner = new Scanner(System.in)) {
      runInputLoop(scanner);
    }

    logger.info("Shutting down...");
  }

  private void runInputLoop(Scanner scanner) {
    while (true) {
      logger.info("Enter a value to append or 'EXIT' to quit:");
      String input = scanner.nextLine();

      if (input.equalsIgnoreCase("EXIT")) {
        break;
      }

      try {
        String response = sendAppendRequest(input);
        if (response == null) {
          logger.info("Request timed out.");
          continue;
        }
        logger.info("response = {}", response);
      } catch (Exception exception) {
        logger.error("Error appending value through the server {}", targetReplicaConfig.id(), exception);
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
      byte[] payload = ProtoValidationUtil.requireValid(request, "ClientRequest").toByteArray();
      transport.send(connectionId, payload, targetAddress);
    } catch (Exception exception) {
      throw new IOException("Could not sign client request", exception);
    }

    try {
      while (true) {
        InboundPacket inbound = receiveNextInbound(transport);
        if (inbound == null) {
          return null;
        }

        String reply = handleReply(inbound.packet(), connectionId);
        if (reply != null) {
          return reply;
        }
      }
    } finally {
      try {
        transport.closeConnection(connectionId, targetAddress);
      } catch (RuntimeException exception) {
        logger.debug("Ignoring client connection close failure", exception);
      }
    }
  }

  private InboundPacket receiveNextInbound(AuthenticatedLink transport) {
    try {
      if (requestTimeoutMs == 0L) {
        while (true) {
          InboundPacket inbound = transport.receive();
          if (inbound != null) {
            return inbound;
          }
        }
      }

      long deadlineMs = TimeUtil.deadlineAfterNow(requestTimeoutMs);
      while (!TimeUtil.hasTimedOut(deadlineMs)) {
        long remainingMs = TimeUtil.remainingMsUntil(deadlineMs);
        InboundPacket inbound = transport.receive(remainingMs);
        if (inbound != null) {
          return inbound;
        }
      }

      return null;
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  private ClientRequest createClientRequest(String value) throws Exception {
    long requestId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    byte[] signature = CryptoUtil.signEcdsa(ClientRequestPayloadUtil.signedAppendRequestPayload(localSenderId, requestId, value), localStaticSKey);
    return ProtoValidationUtil.requireValid(ClientRequest.newBuilder().setAppend(AppendRequest.newBuilder().setRequestKey(
        ClientRequestKey.newBuilder().setClientSenderId(localSenderId).setRequestId(requestId)).setValue(value)
        .setSignature(ByteString.copyFrom(signature))).build(), "ClientRequest");
  }

  private String handleReply(DpchPacket inbound, long connectionId) {
    if (inbound.getConnectionId() != connectionId) {
      return null;
    }

    try {
      ClientResponse response = ProtoValidationUtil.requireValid(ClientResponse.parseFrom(inbound.getPayload()), "ClientResponse");
      if (!response.hasAppend()) {
        throw new IllegalArgumentException("Unsupported client response type: " + response.getBodyCase());
      }
      return response.getAppend().getMessage();
    } catch (com.google.protobuf.InvalidProtocolBufferException exception) {
      throw new IllegalArgumentException("Invalid protobuf client response payload", exception);
    }
  }
}
