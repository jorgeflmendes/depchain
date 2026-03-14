package pt.ulisboa.depchain.shared.network.links.handshaked.registry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import pt.ulisboa.depchain.shared.network.links.handshaked.ConnectionState;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;

public final class ConnectionStateRegistry {
  private final Map<ConnectionKey, ConnectionState> states = new ConcurrentHashMap<>();

  public ConnectionState get(ConnectionKey key) {
    return states.get(key);
  }

  public ConnectionState getOrCreate(ConnectionKey key) {
    return states.computeIfAbsent(key, this::newState);
  }

  public void signalAllStates() {
    for (ConnectionState state : states.values()) {
      state.signalWaiters();
    }
  }

  private ConnectionState newState(ConnectionKey key) {
    ConnectionState[] holder = new ConnectionState[1];
    holder[0] = new ConnectionState(() -> {
      synchronized (holder[0]) {
        holder[0].notifyAll();
      }
    }, () -> states.remove(key, holder[0]));
    return holder[0];
  }
}

