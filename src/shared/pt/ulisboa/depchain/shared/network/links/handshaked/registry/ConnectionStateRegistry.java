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
    return states.computeIfAbsent(key, ignored -> new ConnectionState());
  }

  public void removeIfSame(ConnectionKey key, ConnectionState state) {
    states.remove(key, state);
  }
}
