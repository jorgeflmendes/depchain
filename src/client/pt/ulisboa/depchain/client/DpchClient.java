package pt.ulisboa.depchain.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import pt.ulisboa.depchain.proto.AppendRequest;
import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.ClientResponse;
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

  private final List<ReplicaTarget> replicaTargets;
  private final long localSenderId;
  private final PrivateKey localStaticSKey;
  private final Map<Long, PublicKey> staticPKeys;
  private final long requestTimeoutMs;
  private final int requiredReplyCount;

  public DpchClient(String configPath) throws Exception {
    ConfigParser config = ConfigParser.load(Path.of(configPath));
    this.replicaTargets = config.replicas().stream()
        .map(replica -> new ReplicaTarget(replica.id(), replica.senderId(), new InetSocketAddress(replica.host(), replica.clientPort()))).toList();
    this.localSenderId = config.client().senderId();
    this.localStaticSKey = PrivateKeyLoader.loadClientPrivateKey(config);
    this.staticPKeys = PublicKeyLoader.loadReplicaPublicKeys(config);
    this.requestTimeoutMs = config.client().requestTimeoutMs();
    this.requiredReplyCount = config.system().f() + 1;
  }

  public void run() {
    try (AuthenticatedLink transport = AuthenticatedLink.unbound(localSenderId, localStaticSKey, staticPKeys); Scanner scanner = new Scanner(System.in)) {
      runInputLoop(scanner, transport);
    } catch (Exception exception) {
      throw new IllegalStateException("Failed to initialize client transport", exception);
    }

    logger.info("Shutting down...");
  }

  private void runInputLoop(Scanner scanner, AuthenticatedLink transport) {
    while (true) {
      logger.info("Enter a value to append or 'EXIT' to quit:");
      String input = scanner.nextLine();

      if (input.equalsIgnoreCase("EXIT")) {
        break;
      }

      try {
        String response = sendAppendRequest(transport, input);
        if (response == null) {
          logger.info("Request timed out.");
          continue;
        }
        logger.info("response = {}", response);
      } catch (Exception exception) {
        logger.error("Error broadcasting append request", exception);
      }
    }
  }

  private String sendAppendRequest(AuthenticatedLink transport, String value) throws Exception {
    ClientRequest request = createClientRequest(value);
    byte[] payload = ProtoValidationUtil.requireValid(request, "ClientRequest").toByteArray();
    return runBroadcastRequestLoop(transport, payload);
  }

  private String runBroadcastRequestLoop(AuthenticatedLink transport, byte[] payload) throws IOException, InterruptedException {
    Map<Long, InetSocketAddress> endpointsByConnectionId = new LinkedHashMap<>();
    for (ReplicaTarget replicaTarget : replicaTargets) {
      long connectionId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
      try {
        transport.send(connectionId, payload, replicaTarget.endpoint());
        endpointsByConnectionId.put(connectionId, replicaTarget.endpoint());
      } catch (RuntimeException exception) {
        logger.debug("Ignoring failed client broadcast init to {}", replicaTarget.endpoint(), exception);
      }
    }

    if (endpointsByConnectionId.isEmpty()) {
      return null;
    }

    try {
      long deadlineNanos = requestTimeoutMs == 0L ? Long.MAX_VALUE : TimeUtil.monotonicDeadlineAfterNow(requestTimeoutMs);
      return awaitCoherentReplies(transport, endpointsByConnectionId, deadlineNanos);
    } finally {
      closeClientConnections(transport, endpointsByConnectionId);
    }
  }

  private String awaitCoherentReplies(AuthenticatedLink transport, Map<Long, InetSocketAddress> endpointsByConnectionId, long deadlineNanos) {
    Set<Long> expectedReplicaSenderIds = staticPKeys.keySet();
    Map<Long, String> responseKeyByReplicaSender = new HashMap<>();
    Map<String, ReplyAccumulator> coherentReplies = new HashMap<>();

    while (true) {
      InboundPacket inbound = receiveNextInbound(transport, deadlineNanos);
      if (inbound == null) {
        return null;
      }

      long connectionId = inbound.packet().getConnectionId();
      if (!endpointsByConnectionId.containsKey(connectionId)) {
        continue;
      }

      Long replicaSenderId = inbound.authenticatedSenderId();
      if (replicaSenderId == null || !expectedReplicaSenderIds.contains(replicaSenderId) || responseKeyByReplicaSender.containsKey(replicaSenderId)) {
        continue;
      }

      ClientResponse response = decodeResponse(inbound);
      String responseKey = Base64.getEncoder().encodeToString(ProtoValidationUtil.requireValid(response, "ClientResponse").toByteArray());
      responseKeyByReplicaSender.put(replicaSenderId, responseKey);

      ReplyAccumulator accumulator = coherentReplies.computeIfAbsent(responseKey, ignored -> new ReplyAccumulator(response));
      accumulator.addReplicaSender(replicaSenderId);
      if (accumulator.count() >= requiredReplyCount) {
        return accumulator.response().getAppend().getMessage();
      }
    }
  }

  private InboundPacket receiveNextInbound(AuthenticatedLink transport, long deadlineNanos) {
    try {
      if (requestTimeoutMs == 0L) {
        while (true) {
          InboundPacket inbound = transport.receive();
          if (inbound != null) {
            return inbound;
          }
        }
      }

      while (!TimeUtil.hasTimedOutMonotonic(deadlineNanos)) {
        long remainingMs = TimeUtil.monotonicRemainingMsUntil(deadlineNanos);
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
    return ProtoValidationUtil.requireValid(ClientRequest.newBuilder().setAppend(AppendRequest.newBuilder()
        .setRequestKey(ClientRequestKey.newBuilder().setClientSenderId(localSenderId).setRequestId(requestId)).setValue(value).setSignature(ByteString.copyFrom(signature)))
        .build(), "ClientRequest");
  }

  private ClientResponse decodeResponse(InboundPacket inbound) {
    try {
      ClientResponse response = ProtoValidationUtil.requireValid(ClientResponse.parseFrom(inbound.packet().getPayload()), "ClientResponse");
      if (!response.hasAppend()) {
        throw new IllegalArgumentException("Unsupported client response type: " + response.getBodyCase());
      }
      return response;
    } catch (com.google.protobuf.InvalidProtocolBufferException exception) {
      throw new IllegalArgumentException("Invalid protobuf client response payload", exception);
    }
  }

  private static void closeClientConnections(AuthenticatedLink transport, Map<Long, InetSocketAddress> endpointsByConnectionId) {
    for (Map.Entry<Long, InetSocketAddress> entry : endpointsByConnectionId.entrySet()) {
      try {
        transport.closeConnection(entry.getKey(), entry.getValue());
      } catch (RuntimeException exception) {
        logger.debug("Ignoring client connection close failure", exception);
      }
    }
  }

  private record ReplicaTarget(String replicaId, long senderId, InetSocketAddress endpoint) {
  }

  private static final class ReplyAccumulator {
    private final ClientResponse response;
    private final List<Long> replicaSenderIds = new ArrayList<>();

    private ReplyAccumulator(ClientResponse response) {
      this.response = response;
    }

    private void addReplicaSender(long replicaSenderId) {
      replicaSenderIds.add(replicaSenderId);
    }

    private int count() {
      return replicaSenderIds.size();
    }

    private ClientResponse response() {
      return response;
    }
  }
}
