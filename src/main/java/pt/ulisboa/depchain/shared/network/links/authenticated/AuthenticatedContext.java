package pt.ulisboa.depchain.shared.network.links.authenticated;

import static pt.ulisboa.depchain.shared.validation.ValidationUtils.named;

import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import pt.ulisboa.depchain.shared.network.links.AsyncLinkContext;
import pt.ulisboa.depchain.shared.network.links.perfect.PerfectLink;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.validation.ValidationUtils;

final class AuthenticatedContext extends AsyncLinkContext<InboundPacket> {
  private static final long WORKER_WAIT_MS = 100L;

  final PerfectLink perfectLink;

  final long localSenderId;
  final PrivateKey localStaticSKey;
  final Map<Long, PublicKey> staticPKeys;
  final Map<ConnectionKey, AuthenticatedConnectionState> connectionStates = new ConcurrentHashMap<>();
  private final Object receiveModeLock = new Object();
  private int pendingAsyncHandshakes;
  private boolean directReceiveActive;

  AuthenticatedContext(PerfectLink perfectLink, long localSenderId, PrivateKey localStaticSKey, Map<Long, PublicKey> staticPKeys) {
    ValidationUtils.requireAllNonNull(named("perfectLink", perfectLink), named("localStaticSKey", localStaticSKey), named("staticPKeys", staticPKeys));
    this.perfectLink = perfectLink;
    this.localSenderId = ValidationUtils.requireNonNegativeLong(localSenderId, "localSenderId");
    this.localStaticSKey = localStaticSKey;
    this.staticPKeys = Map.copyOf(staticPKeys);
  }

  AuthenticatedConnectionState getOrCreateConnectionState(ConnectionKey connectionKey) {
    ValidationUtils.requireNonNull(connectionKey, "connectionKey");
    return connectionStates.computeIfAbsent(connectionKey, key -> new AuthenticatedConnectionState(() -> connectionStates.remove(key)));
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

  void signalAsyncHandshakeStarted() {
    synchronized (receiveModeLock) {
      pendingAsyncHandshakes++;
      if (!directReceiveActive) {
        receiveModeLock.notifyAll();
      }
    }
  }

  void signalAsyncHandshakeCompleted() {
    synchronized (receiveModeLock) {
      if (pendingAsyncHandshakes > 0) {
        pendingAsyncHandshakes--;
      }
      if (!directReceiveActive) {
        receiveModeLock.notifyAll();
      }
    }
  }

  boolean tryEnterDirectReceive() {
    synchronized (receiveModeLock) {
      if (!isRunning() || pendingAsyncHandshakes > 0 || directReceiveActive) {
        return false;
      }
      directReceiveActive = true;
      return true;
    }
  }

  void exitDirectReceive() {
    synchronized (receiveModeLock) {
      directReceiveActive = false;
      receiveModeLock.notifyAll();
    }
  }

  boolean awaitHandshakeWork() throws InterruptedException {
    synchronized (receiveModeLock) {
      while (isRunning() && (pendingAsyncHandshakes == 0 || directReceiveActive)) {
        receiveModeLock.wait(WORKER_WAIT_MS);
      }
      return isRunning() && pendingAsyncHandshakes > 0 && !directReceiveActive;
    }
  }

  PublicKey getStaticPublicKey(long senderId) {
    return staticPKeys.get(senderId);
  }

  void shutdown() {
    synchronized (receiveModeLock) {
      receiveModeLock.notifyAll();
    }
    shutdownInbox();
  }
}
