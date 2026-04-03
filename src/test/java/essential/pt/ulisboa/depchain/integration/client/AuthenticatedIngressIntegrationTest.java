package pt.ulisboa.depchain.integration.client;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Map;

import javax.crypto.SecretKey;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pt.ulisboa.depchain.integration.support.IntegrationHarness;
import pt.ulisboa.depchain.integration.support.IntegrationHarness.ManagedCluster;
import pt.ulisboa.depchain.proto.AuthOpcode;
import pt.ulisboa.depchain.proto.DpchPacket;
import pt.ulisboa.depchain.proto.DpchPacketType;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.crypto.key.PrivateKeyLoader;
import pt.ulisboa.depchain.shared.crypto.key.PublicKeyLoader;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedPayloadUtil;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;

@Tag("integration")
class AuthenticatedIngressIntegrationTest extends IntegrationHarness {

  @Test
  void invalidHmacPacketIsDroppedWithoutBreakingSubsequentClientTraffic() throws Exception {
    try (ManagedCluster cluster = startManagedCluster(REPLICA_IDS)) {
      ConfigParser config = ConfigParser.load(cluster.configPath());
      ConfigParser.ReplicaSection leader = config.requireReplicaById(LEADER_REPLICA_ID);
      InetSocketAddress endpoint = new InetSocketAddress(leader.host(), leader.clientPort());
      long connectionId = 41L;

      try (AuthenticatedLink transport = clientTransport(config)) {
        primeAuthenticatedSession(transport, endpoint, connectionId);

        SecretKey sharedSecret = sharedSecretForConnection(transport, endpoint, connectionId);
        int nextSequence = nextSequenceForConnection(transport, endpoint, connectionId);
        long sentNonce = sentNonceForConnection(transport, endpoint, connectionId);
        byte[] invalidEnvelope = AuthenticatedPayloadUtil.encodeHmac(AuthOpcode.AUTH_OPCODE_DATA, "tampered-hmac".getBytes(StandardCharsets.UTF_8), sharedSecret, sentNonce + 1L);
        invalidEnvelope[invalidEnvelope.length - 1] ^= 0x01;

        sendRawDpchPacket(transport, endpoint, connectionId, nextSequence, invalidEnvelope);
      }

      cluster.assertRequestSucceeds("transfer-after-invalid-hmac", STANDARD_REQUEST_TIMEOUT, "Cluster should keep accepting valid traffic after dropping invalid HMAC");
    }
  }

  @Test
  void replayedAuthenticatedNonceIsDroppedWithoutBreakingSubsequentClientTraffic() throws Exception {
    try (ManagedCluster cluster = startManagedCluster(REPLICA_IDS)) {
      ConfigParser config = ConfigParser.load(cluster.configPath());
      ConfigParser.ReplicaSection leader = config.requireReplicaById(LEADER_REPLICA_ID);
      InetSocketAddress endpoint = new InetSocketAddress(leader.host(), leader.clientPort());
      long connectionId = 42L;

      try (AuthenticatedLink transport = clientTransport(config)) {
        primeAuthenticatedSession(transport, endpoint, connectionId);

        SecretKey sharedSecret = sharedSecretForConnection(transport, endpoint, connectionId);
        int nextSequence = nextSequenceForConnection(transport, endpoint, connectionId);
        long sentNonce = sentNonceForConnection(transport, endpoint, connectionId);
        byte[] replayEnvelope = AuthenticatedPayloadUtil.encodeHmac(AuthOpcode.AUTH_OPCODE_DATA, "replayed-nonce".getBytes(StandardCharsets.UTF_8), sharedSecret, sentNonce);

        sendRawDpchPacket(transport, endpoint, connectionId, nextSequence, replayEnvelope);
      }

      cluster
          .assertRequestSucceeds("transfer-after-replayed-nonce", STANDARD_REQUEST_TIMEOUT, "Cluster should keep accepting valid traffic after dropping replayed authenticated data");
    }
  }

  private static AuthenticatedLink clientTransport(ConfigParser config) throws Exception {
    long clientSenderId = config.client().senderId();
    PrivateKey clientPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config);
    Map<Long, PublicKey> staticPublicKeys = PublicKeyLoader.loadStaticPublicKeys(config);
    return AuthenticatedLink.unbound(clientSenderId, clientPrivateKey, staticPublicKeys);
  }

  private static void primeAuthenticatedSession(AuthenticatedLink transport, InetSocketAddress endpoint, long connectionId) throws Exception {
    transport.send(connectionId, "seed-authenticated-session".getBytes(StandardCharsets.UTF_8), endpoint);
    await().forever().until(() -> sharedSecretForConnection(transport, endpoint, connectionId) != null);
    await().forever().until(() -> nextSequenceForConnection(transport, endpoint, connectionId) >= 2);
  }

  private static void sendRawDpchPacket(AuthenticatedLink transport, InetSocketAddress endpoint, long connectionId, int sequenceNumber, byte[] payload) throws Exception {
    DpchPacket packet = DpchPacket.newBuilder().setConnectionId(connectionId).setSequenceNumber(sequenceNumber).setPacketType(DpchPacketType.DPCH_PACKET_TYPE_DATA)
        .setPayload(com.google.protobuf.ByteString.copyFrom(payload)).build();
    Object fairLossLink = fairLossLink(transport);
    Method sendMethod = fairLossLink.getClass().getMethod("send", byte[].class, InetSocketAddress.class);
    sendMethod.invoke(fairLossLink, packet.toByteArray(), endpoint);
  }

  private static SecretKey sharedSecretForConnection(AuthenticatedLink transport, InetSocketAddress endpoint, long connectionId) throws Exception {
    Object connectionState = authenticatedConnectionState(transport, endpoint, connectionId);
    if (connectionState == null) {
      return null;
    }
    Method method = connectionState.getClass().getDeclaredMethod("sharedSecret");
    method.setAccessible(true);
    return (SecretKey) method.invoke(connectionState);
  }

  private static long sentNonceForConnection(AuthenticatedLink transport, InetSocketAddress endpoint, long connectionId) throws Exception {
    Object connectionState = authenticatedConnectionState(transport, endpoint, connectionId);
    assertNotNull(connectionState);
    return ((Number) getFieldValue(connectionState, "sentNonce")).longValue();
  }

  private static int nextSequenceForConnection(AuthenticatedLink transport, InetSocketAddress endpoint, long connectionId) throws Exception {
    Object perfectConnectionState = perfectConnectionState(transport, endpoint, connectionId);
    if (perfectConnectionState == null) {
      return 0;
    }
    Method senderStateMethod = perfectConnectionState.getClass().getDeclaredMethod("senderState");
    senderStateMethod.setAccessible(true);
    Object senderState = senderStateMethod.invoke(perfectConnectionState);
    return ((Number) getFieldValue(senderState, "nextSequence")).intValue();
  }

  private static Object authenticatedConnectionState(AuthenticatedLink transport, InetSocketAddress endpoint, long connectionId) throws Exception {
    Object context = getFieldValue(transport, "context");
    @SuppressWarnings("unchecked")
    Map<ConnectionKey, Object> states = (Map<ConnectionKey, Object>) getFieldValue(context, "connectionStates");
    return states.get(new ConnectionKey(endpoint, connectionId));
  }

  private static Object perfectConnectionState(AuthenticatedLink transport, InetSocketAddress endpoint, long connectionId) throws Exception {
    Object context = getFieldValue(transport, "context");
    Object perfectLink = getFieldValue(context, "perfectLink");
    Object perfectContext = getFieldValue(perfectLink, "context");
    @SuppressWarnings("unchecked")
    Map<ConnectionKey, Object> states = (Map<ConnectionKey, Object>) getFieldValue(perfectContext, "connectionStates");
    return states.get(new ConnectionKey(endpoint, connectionId));
  }

  private static Object fairLossLink(AuthenticatedLink transport) throws Exception {
    Object context = getFieldValue(transport, "context");
    Object perfectLink = getFieldValue(context, "perfectLink");
    Object perfectContext = getFieldValue(perfectLink, "context");
    Object stubbornLink = getFieldValue(perfectContext, "stubbornLink");
    Object stubbornContext = getFieldValue(stubbornLink, "context");
    return getFieldValue(stubbornContext, "fairLossLink");
  }

  private static Object getFieldValue(Object target, String fieldName) throws Exception {
    Field field = findField(target.getClass(), fieldName);
    field.setAccessible(true);
    return field.get(target);
  }

  private static Field findField(Class<?> type, String fieldName) throws NoSuchFieldException {
    Class<?> current = type;
    while (current != null) {
      try {
        return current.getDeclaredField(fieldName);
      } catch (NoSuchFieldException ignored) {
        current = current.getSuperclass();
      }
    }
    throw new NoSuchFieldException(fieldName);
  }
}
