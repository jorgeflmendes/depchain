package pt.ulisboa.depchain.shared.network.links.handshaked.registry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.utils.TimeUtil;

// Registry to track recently closed connections to prevent immediate re-establishment and potential resource exhaustion
public final class ClosedConnectionsRegistry {
  private final long connectionIdleTtlMs;
  private final Map<ConnectionKey, Long> closedStates = new ConcurrentHashMap<>();

  public ClosedConnectionsRegistry(long connectionIdleTtlMs) {
    this.connectionIdleTtlMs = connectionIdleTtlMs;
  }

  public void markClosed(ConnectionKey key, long now) {
    closedStates.put(key, now);
  }

  public boolean isClosedRecently(ConnectionKey key, long now) {
    Long closedAtMs = closedStates.get(key);
    if (closedAtMs == null) {
      return false;
    }

    if (TimeUtil.hasElapsedMoreThan(now, closedAtMs, connectionIdleTtlMs)) { // Expired.
      closedStates.remove(key, closedAtMs);
      return false;
    }

    return true;
  }

  public void cleanup(long now) {
    if (closedStates.isEmpty()) {
      return;
    }

    closedStates.entrySet().removeIf(entry -> TimeUtil.hasElapsedMoreThan(now, entry.getValue(), connectionIdleTtlMs));
  }
}
