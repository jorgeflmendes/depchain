package pt.ulisboa.depchain.shared.network.links.handshaked.registry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import pt.ulisboa.depchain.shared.network.links.handshaked.ConnectionState;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;

// Registry to track recently closed connections to prevent immediate re-establishment and potential resource exhaustion
public final class ClosedConnectionsRegistry {
  private final long connectionIdleTtlMs;
  private final int maxConnectionStates;
  private final Map<ConnectionKey, Long> closedStates = new ConcurrentHashMap<>();

  public ClosedConnectionsRegistry(long connectionIdleTtlMs, int maxConnectionStates) {
    this.connectionIdleTtlMs = connectionIdleTtlMs;
    this.maxConnectionStates = maxConnectionStates;
  }

  public void markClosed(ConnectionKey key, long now) {
    closedStates.put(key, now);
  }

  public boolean isClosedRecently(ConnectionKey key, long now) {
    Long closedAtMs = closedStates.get(key);
    if (closedAtMs == null) {
      return false;
    }
    if ((now - closedAtMs) > connectionIdleTtlMs) {
      closedStates.remove(key, closedAtMs);
      return false;
    }
    return true;
  }

  public void cleanup(long now, Map<ConnectionKey, ConnectionState> connectionStates) {
    if (closedStates.isEmpty()) {
      return;
    }

    closedStates.entrySet().removeIf(entry -> (now - entry.getValue()) > connectionIdleTtlMs);
    if (closedStates.size() <= maxConnectionStates) {
      return;
    }

    closedStates.entrySet().removeIf(entry -> {
      if (closedStates.size() <= maxConnectionStates) {
        return false;
      }
      return !connectionStates.containsKey(entry.getKey());
    });
  }
}
