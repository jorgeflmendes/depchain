package pt.ulisboa.depchain.shared.network.links.handshaked;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import pt.ulisboa.depchain.shared.network.links.AsyncLinkContext;
import pt.ulisboa.depchain.shared.network.links.perfect.PerfectLink;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class HandshakedContext extends AsyncLinkContext<InboundPacket> {
  final PerfectLink perfectLink;
  final Map<ConnectionKey, ConnectionState> connectionStates = new ConcurrentHashMap<>();

  HandshakedContext(PerfectLink perfectLink) {
    this.perfectLink = ValidationUtils.requireNonNull(perfectLink, "perfectLink");
  }

  ConnectionState getConnectionState(ConnectionKey connectionKey) {
    return connectionStates.get(connectionKey);
  }

  ConnectionState getOrCreateConnectionState(ConnectionKey connectionKey) {
    return connectionStates.computeIfAbsent(connectionKey, this::newConnectionState);
  }

  void shutdown() {
    shutdownInbox();
    for (ConnectionState connectionState : connectionStates.values()) {
      connectionState.signalWaiters();
    }
  }

  private ConnectionState newConnectionState(ConnectionKey connectionKey) {
    ConnectionState[] holder = new ConnectionState[1];
    holder[0] = new ConnectionState(() -> {
      synchronized (holder[0]) {
        holder[0].notifyAll();
      }
    }, () -> connectionStates.remove(connectionKey, holder[0]));
    return holder[0];
  }
}
