package pt.ulisboa.depchain.shared.network.links.handshaked;

// Per-stream handshake lifecycle for local and remote sides.
final class ConnectionState {
  private SideState local = SideState.NEW;
  private SideState remote = SideState.NEW;
  private volatile long lastTouchedAtMs = System.currentTimeMillis();

  void touch(long now) {
    lastTouchedAtMs = now;
  }

  boolean shouldSendSyn() {
    return local == SideState.NEW;
  }

  void markLocalEstablished() {
    if (local == SideState.NEW) {
      local = SideState.ESTABLISHED;
    }
  }

  boolean isLocalFinished() {
    return local == SideState.FINISHED;
  }

  boolean isRemoteFinished() {
    return remote == SideState.FINISHED;
  }

  void markLocalFinished() {
    local = SideState.FINISHED;
  }

  void markRemoteEstablishedIfNotFinished() {
    if (remote != SideState.FINISHED) {
      remote = SideState.ESTABLISHED;
    }
  }

  void markRemoteFinished() {
    remote = SideState.FINISHED;
  }

  boolean isFullyEstablished() {
    return local == SideState.ESTABLISHED && remote == SideState.ESTABLISHED;
  }

  boolean isStale(long now, long ttlMs) {
    return (now - lastTouchedAtMs) >= ttlMs;
  }

  boolean isFinished() {
    return local == SideState.FINISHED || remote == SideState.FINISHED;
  }

  private enum SideState {
    NEW,
    ESTABLISHED,
    FINISHED
  }
}
