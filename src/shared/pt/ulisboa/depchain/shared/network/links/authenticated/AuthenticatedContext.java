package pt.ulisboa.depchain.shared.network.links.authenticated;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.crypto.SecretKey;

import pt.ulisboa.depchain.shared.network.links.handshaked.HandshakedPerfectLink;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class AuthenticatedContext {
  static final class ConnectionNonceState {
    private final AtomicLong sentNonce = new AtomicLong(0L);
    private final AtomicLong receivedNonce = new AtomicLong(0L);

    long getSentNonce() {
      return sentNonce.get();
    }

    long getReceivedNonce() {
      return receivedNonce.get();
    }

    long incrementSentNonce() {
      return sentNonce.incrementAndGet();
    }

    long incrementReceivedNonce() {
      return receivedNonce.incrementAndGet();
    }
  }

  final HandshakedPerfectLink handshakedLink;

  // Local node information
  final long localSenderId;
  final PrivateKey localStaticSKey;

  // Every node's static public key, indexed by sender ID
  final Map<Long, PublicKey> staticPKeys;

  // Per-connection state
  final Map<ConnectionKey, ConnectionNonceState> nonceStateByConnection = new ConcurrentHashMap<>();
  final Map<ConnectionKey, PrivateKey> ephemeralPrivateKeyByConnection = new ConcurrentHashMap<>();
  final Map<ConnectionKey, SecretKey> sharedSecretByConnection = new ConcurrentHashMap<>();
  final Map<ConnectionKey, Queue<byte[]>> pendingPayloadsByConnection = new ConcurrentHashMap<>();

  AuthenticatedContext(HandshakedPerfectLink handshakedLink, long localSenderId, PrivateKey localStaticSKey, Map<Long, PublicKey> staticPKeys) {
    ValidationUtils.requireAllNonNull(named("handshakedLink", handshakedLink), named("localStaticSKey", localStaticSKey), named("staticPKeys", staticPKeys));
    this.handshakedLink = handshakedLink;
    this.localSenderId = ValidationUtils.requireNonNegativeLong(localSenderId, "localSenderId");
    this.localStaticSKey = localStaticSKey;
    this.staticPKeys = Map.copyOf(staticPKeys);
  }

  void ensureNonceState(ConnectionKey connectionKey) {
    ValidationUtils.requireNonNull(connectionKey, "connectionKey");
    getOrCreateConnectionNonceState(connectionKey);
  }

  long getSentNonce(ConnectionKey connectionKey) {
    ValidationUtils.requireNonNull(connectionKey, "connectionKey");
    ConnectionNonceState nonceState = nonceStateByConnection.get(connectionKey);
    if (nonceState == null) {
      throw new IllegalStateException("Missing nonce state for connection " + connectionKey);
    }
    return nonceState.getSentNonce();
  }

  boolean hasNonceState(ConnectionKey connectionKey) {
    return nonceStateByConnection.containsKey(connectionKey);
  }

  long incrementSentNonce(ConnectionKey connectionKey) {
    return getOrCreateConnectionNonceState(connectionKey).incrementSentNonce();
  }

  long getReceivedNonce(ConnectionKey connectionKey) {
    ValidationUtils.requireNonNull(connectionKey, "connectionKey");
    ConnectionNonceState nonceState = nonceStateByConnection.get(connectionKey);
    if (nonceState == null) {
      throw new IllegalStateException("Missing nonce state for connection " + connectionKey);
    }
    return nonceState.getReceivedNonce();
  }

  long incrementReceivedNonce(ConnectionKey connectionKey) {
    return getOrCreateConnectionNonceState(connectionKey).incrementReceivedNonce();
  }

  boolean verifyReceivedNonce(ConnectionKey connectionKey, long nonce) {
    return nonce > getReceivedNonce(connectionKey);
  }

  PublicKey getStaticPublicKey(long senderId) {
    return staticPKeys.get(senderId);
  }

  void putSharedSecret(ConnectionKey connectionKey, SecretKey sharedSecret) {
    ValidationUtils.requireAllNonNull(named("connectionKey", connectionKey), named("sharedSecret", sharedSecret));
    sharedSecretByConnection.put(connectionKey, sharedSecret);
  }

  boolean hasSharedSecret(ConnectionKey connectionKey) {
    return sharedSecretByConnection.containsKey(connectionKey);
  }

  SecretKey getSharedSecret(ConnectionKey connectionKey) {
    return sharedSecretByConnection.get(connectionKey);
  }

  void putEphemeralPrivateKey(ConnectionKey connectionKey, PrivateKey ephemeralPrivateKey) {
    ValidationUtils.requireAllNonNull(named("connectionKey", connectionKey), named("ephemeralPrivateKey", ephemeralPrivateKey));
    ephemeralPrivateKeyByConnection.put(connectionKey, ephemeralPrivateKey);
  }

  boolean hasEphemeralPrivateKey(ConnectionKey connectionKey) {
    return ephemeralPrivateKeyByConnection.containsKey(connectionKey);
  }

  PrivateKey getEphemeralPrivateKey(ConnectionKey connectionKey) {
    return ephemeralPrivateKeyByConnection.get(connectionKey);
  }

  void removeEphemeralPrivateKey(ConnectionKey connectionKey) {
    ValidationUtils.requireNonNull(connectionKey, "connectionKey");
    ephemeralPrivateKeyByConnection.remove(connectionKey);
  }

  void enqueuePendingPayload(ConnectionKey connectionKey, byte[] payload) {
    ValidationUtils.requireAllNonNull(named("connectionKey", connectionKey), named("payload", payload));
    pendingPayloadsByConnection.computeIfAbsent(connectionKey, ignored -> new ConcurrentLinkedQueue<>()).add(Arrays.copyOf(payload, payload.length));
  }

  Queue<byte[]> removePendingPayloads(ConnectionKey connectionKey) {
    ValidationUtils.requireNonNull(connectionKey, "connectionKey");
    Queue<byte[]> pendingPayloads = pendingPayloadsByConnection.remove(connectionKey);
    if (pendingPayloads == null) {
      return new ConcurrentLinkedQueue<>();
    }
    return pendingPayloads;
  }

  private ConnectionNonceState getOrCreateConnectionNonceState(ConnectionKey connectionKey) {
    return nonceStateByConnection.computeIfAbsent(connectionKey, ignored -> new ConnectionNonceState());
  }
}
