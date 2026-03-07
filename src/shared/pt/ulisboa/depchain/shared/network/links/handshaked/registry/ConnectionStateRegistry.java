package pt.ulisboa.depchain.shared.network.links.handshaked.registry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import pt.ulisboa.depchain.shared.network.links.handshaked.ConnectionState;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;

// Registry to track active connection states and manage their lifecycle, including cleanup of stale states
public final class ConnectionStateRegistry {
  private final long connectionIdleTtlMs;
  private final Map<ConnectionKey, ConnectionState> states = new ConcurrentHashMap<>();

  public ConnectionStateRegistry(long connectionIdleTtlMs) {
    this.connectionIdleTtlMs = connectionIdleTtlMs;
  }

  public ConnectionState get(ConnectionKey key) {
    return states.get(key);
  }

  public ConnectionState getOrCreate(ConnectionKey key) {
    return states.computeIfAbsent(key, ignored -> new ConnectionState());
  }

  public void removeIfSame(ConnectionKey key, ConnectionState state) {
    states.remove(key, state);
  }

  public void cleanup(long now, ClosedConnectionsRegistry closedConnections) {
    closedConnections.cleanup(now);
    states.entrySet().removeIf(entry -> entry.getValue().isStale(now, connectionIdleTtlMs));
  }
}
