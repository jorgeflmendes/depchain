package pt.ulisboa.depchain.shared.network.links.authenticated;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import pt.ulisboa.depchain.shared.network.links.AsyncLinkContext;
import pt.ulisboa.depchain.shared.network.links.handshaked.HandshakedPerfectLink;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class AuthenticatedContext extends AsyncLinkContext<InboundPacket> {
  final HandshakedPerfectLink handshakedLink;

  final long localSenderId;
  final PrivateKey localStaticSKey;
  final Map<Long, PublicKey> staticPKeys;
  final Map<ConnectionKey, AuthenticatedConnectionState> connectionStates = new ConcurrentHashMap<>();

  AuthenticatedContext(HandshakedPerfectLink handshakedLink, long localSenderId, PrivateKey localStaticSKey, Map<Long, PublicKey> staticPKeys) {
    ValidationUtils.requireAllNonNull(named("handshakedLink", handshakedLink), named("localStaticSKey", localStaticSKey), named("staticPKeys", staticPKeys));
    this.handshakedLink = handshakedLink;
    this.localSenderId = ValidationUtils.requireNonNegativeLong(localSenderId, "localSenderId");
    this.localStaticSKey = localStaticSKey;
    this.staticPKeys = Map.copyOf(staticPKeys);
  }

  AuthenticatedConnectionState getOrCreateConnectionState(ConnectionKey connectionKey) {
    ValidationUtils.requireNonNull(connectionKey, "connectionKey");
    return connectionStates.computeIfAbsent(connectionKey, this::newConnectionState);
  }

  AuthenticatedConnectionState getConnectionStateOrNull(ConnectionKey connectionKey) {
    ValidationUtils.requireNonNull(connectionKey, "connectionKey");
    return connectionStates.get(connectionKey);
  }

  void closeAllConnectionStates() {
    for (AuthenticatedConnectionState connectionState : connectionStates.values()) {
      connectionState.close();
    }
  }

  PublicKey getStaticPublicKey(long senderId) {
    return staticPKeys.get(senderId);
  }

  void shutdown() {
    shutdownInbox();
  }

  private AuthenticatedConnectionState newConnectionState(ConnectionKey connectionKey) {
    return new AuthenticatedConnectionState(() -> connectionStates.remove(connectionKey));
  }
}
