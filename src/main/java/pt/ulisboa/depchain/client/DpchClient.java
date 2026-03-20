package pt.ulisboa.depchain.client;

import java.net.InetSocketAddress;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import pt.ulisboa.depchain.proto.AppendRequest;
import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.ClientResponse;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.keys.PrivateKeyLoader;
import pt.ulisboa.depchain.shared.keys.PublicKeyLoader;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.ClientRequestSignaturePayloadUtil;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;
import pt.ulisboa.depchain.shared.utils.ProtoValidationUtil;
import pt.ulisboa.depchain.shared.utils.QuorumAccumulator;
import pt.ulisboa.depchain.shared.utils.TimeUtil;

public final class DpchClient implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(DpchClient.class);
  private static final SecureRandom connectionIdRandom = new SecureRandom();

  private List<InetSocketAddress> replicaEndpoints;
  private PrivateKey clientPrivateKey;
  private Map<Long, PublicKey> replicaPublicKeys;
  private long requestTimeoutMs;
  private int coherentReplyQuorum;
  private long clientSenderId;
  private AuthenticatedLink transport;
  private Map<Long, InetSocketAddress> openConnections;
  private AtomicLong nextRequestNonce;

  public DpchClient(String configPath) throws Exception {
    ConfigParser config = ConfigParser.load(java.nio.file.Path.of(configPath));
    this.replicaEndpoints = config.replicas().stream().map(replica -> new InetSocketAddress(replica.host(), replica.clientPort())).toList();
    this.clientSenderId = config.client().senderId();
    this.clientPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config);
    this.replicaPublicKeys = PublicKeyLoader.loadReplicaPublicKeys(config);
    this.requestTimeoutMs = config.client().requestTimeoutMs();
    this.coherentReplyQuorum = config.system().f() + 1;
    this.transport = AuthenticatedLink.unbound(this.clientSenderId, this.clientPrivateKey, this.replicaPublicKeys);
    this.openConnections = new LinkedHashMap<>();
    this.nextRequestNonce = new AtomicLong(0L);
  }

  public static DpchClient open(String configPath) throws Exception {
    return new DpchClient(configPath);
  }

  // TODO: implement the client-side transaction command flow using
  // TransactionRequest/TransactionResponse.
  public String requestAppend(String value) throws Exception {
    long nonce = nextRequestNonce.getAndIncrement();
    byte[] signature = CryptoUtil.signEcdsa(ClientRequestSignaturePayloadUtil.signedAppendRequestPayload(clientSenderId, nonce, value), clientPrivateKey);

    // Make the proto request and validate
    ClientRequest request = ClientRequest.newBuilder().setAppend(AppendRequest.newBuilder()
        .setRequestKey(ClientRequestKey.newBuilder().setClientSenderId(clientSenderId).setRequestId(nonce)).setValue(value).setSignature(ByteString.copyFrom(signature))).build();
    byte[] payload = ProtoValidationUtil.requireValid(request, "ClientRequest").toByteArray();

    openConnections.clear();
    for (InetSocketAddress replica : replicaEndpoints) {
      // Get a unique connection ID
      long connectionId;
      while (true) {
        connectionId = connectionIdRandom.nextLong();
        if (connectionId != 0L && !openConnections.containsKey(connectionId)) {
          break;
        }
      }

      // Send the request, ignoring send failures (which may be due to some replicas being down).
      try {
        transport.send(connectionId, payload, replica);
        openConnections.put(connectionId, replica);
      } catch (RuntimeException exception) {
        logger.debug("Ignoring failed client broadcast init to {}", replica, exception);
      }
    }

    if (openConnections.isEmpty()) {
      return null;
    }

    long deadlineNanos = Long.MAX_VALUE;
    if (requestTimeoutMs != 0L) {
      deadlineNanos = TimeUtil.monotonicDeadlineAfterNow(requestTimeoutMs);
    }

    // Wait for replies, and return the first value for which a quorum of replicas replied with it.
    try {
      return receiveAppend(deadlineNanos);
    } finally {
      for (Map.Entry<Long, InetSocketAddress> entry : openConnections.entrySet()) {
        try {
          transport.closeConnection(entry.getKey(), entry.getValue());
        } catch (RuntimeException exception) {
          logger.debug("Ignoring client connection close failure", exception);
        }
      }

      openConnections.clear();
    }
  }

  public String receiveAppend(long deadlineNanos) {
    QuorumAccumulator<Long, String, ClientResponse> repliesByValue = new QuorumAccumulator<>(replicaPublicKeys.keySet());

    while (true) {
      // Receive a reply, ignoring receive failures (which may be due to some replicas being down).
      InboundPacket inbound;
      try {
        if (requestTimeoutMs == 0L) {
          do {
            inbound = transport.receive();
          } while (inbound == null);
        } else {
          inbound = null;
          while (!TimeUtil.hasTimedOutMonotonic(deadlineNanos) && inbound == null) {
            inbound = transport.receive(TimeUtil.monotonicRemainingMsUntil(deadlineNanos));
          }
        }
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        return null;
      } catch (Exception exception) {
        logger.debug("Ignoring client receive failure", exception);
        return null;
      }

      if (inbound == null) {
        return null;
      }

      if (!openConnections.containsKey(inbound.packet().getConnectionId())) {
        continue;
      }

      Long replicaSenderId = inbound.authenticatedSenderId();
      if (replicaSenderId == null) {
        continue;
      }

      // Parse and validate the reply payload
      ClientResponse response;
      try {
        response = ProtoValidationUtil.requireValid(ClientResponse.parseFrom(inbound.payload()), "ClientResponse");
        if (!response.hasAppend()) {
          throw new IllegalArgumentException("Unsupported client response type: " + response.getBodyCase());
        }
      } catch (InvalidProtocolBufferException exception) {
        throw new IllegalArgumentException("Invalid protobuf client response payload", exception);
      }
      ClientResponse quorumResponse = repliesByValue.recordAndGetFirstValueIfQuorumReached(replicaSenderId, Base64.getEncoder()
          .encodeToString(ProtoValidationUtil.requireValid(response, "ClientResponse").toByteArray()), response, coherentReplyQuorum);

      // If a quorum of replicas has replied with the same value, return it.
      if (quorumResponse != null) {
        return quorumResponse.getAppend().getMessage();
      }
    }
  }

  @Override
  public void close() throws Exception {
    transport.close();
  }
}
