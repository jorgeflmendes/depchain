package pt.ulisboa.depchain.shared.network.links.handshaked;

import pt.ulisboa.depchain.shared.utils.TimeUtil;

// Per-stream handshake lifecycle for local and remote sides.
public final class ConnectionState {
  private enum SideState {
    NEW,
    ESTABLISHED,
    FINISHED
  }

  private volatile long lastTouchedAtMs;

  // Local and remote states for the handshake lifecycle of each side of the connection.
  private SideState local = SideState.NEW;
  private SideState remote = SideState.NEW;

  // If a local close has been requested by the application.
  private boolean localCloseRequested;

  public ConnectionState() {
    this.localCloseRequested = false;
    this.lastTouchedAtMs = System.currentTimeMillis();
  }

  public void touch(long now) {
    lastTouchedAtMs = now;
  }

  public void markLocalEstablished() {
    if (local == SideState.NEW) {
      local = SideState.ESTABLISHED;
    }
  }

  public void requestLocalClose() {
    localCloseRequested = true;
  }

  public void markLocalFinished() {
    local = SideState.FINISHED;
  }

  public void markRemoteEstablishedIfNotFinished() {
    if (remote != SideState.FINISHED) {
      remote = SideState.ESTABLISHED;
    }
  }

  public void markRemoteFinished() {
    remote = SideState.FINISHED;
  }

  public boolean shouldSendSyn() {
    return local == SideState.NEW;
  }

  public boolean shouldSendFin() {
    return localCloseRequested && local != SideState.FINISHED;
  }

  public boolean isFullyEstablished() {
    return local == SideState.ESTABLISHED && remote == SideState.ESTABLISHED;
  }

  public boolean canExchangeData() {
    return isFullyEstablished() && !isClosing();
  }

  public boolean isLocalFinished() {
    return local == SideState.FINISHED;
  }

  public boolean isRemoteFinished() {
    return remote == SideState.FINISHED;
  }

  public boolean isLocalCloseRequested() {
    return localCloseRequested;
  }

  public boolean isStale(long now, long ttlMs) {
    return TimeUtil.hasElapsedAtLeast(now, lastTouchedAtMs, ttlMs); // Last touch is greater/equal to the configured TTL for staleness.
  }

  public boolean isCloseConverged() {
    return local == SideState.FINISHED && remote == SideState.FINISHED;
  }

  public boolean isClosing() {
    return localCloseRequested || local == SideState.FINISHED || remote == SideState.FINISHED;
  }
}
