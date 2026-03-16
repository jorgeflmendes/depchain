package pt.ulisboa.depchain.shared.network.links.handshaked;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import pt.ulisboa.depchain.shared.network.links.RunOnce;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

// Per-stream handshake lifecycle for local and remote sides.
public final class ConnectionState {
  private enum SideState {
    NEW, ESTABLISHED, FINISHED
  }

  // Local and remote states for the handshake lifecycle of each side of the connection.
  private SideState local = SideState.NEW;
  private SideState remote = SideState.NEW;

  // If a local close has been requested by the application.
  private boolean localCloseRequested;
  private final Runnable onStateChange;
  private final RunOnce onTerminal;

  public ConnectionState() {
    this(() -> {
    }, () -> {
    });
  }

  public ConnectionState(Runnable onStateChange, Runnable onTerminal) {
    ValidationUtils.requireAllNonNull(named("onStateChange", onStateChange), named("onTerminal", onTerminal));
    this.onStateChange = onStateChange;
    this.onTerminal = new RunOnce(onTerminal);
    this.localCloseRequested = false;
  }

  public void markLocalEstablished() {
    if (local == SideState.NEW) {
      local = SideState.ESTABLISHED;
      signalStateChange();
    }
  }

  public void requestLocalClose() {
    if (!localCloseRequested) {
      localCloseRequested = true;
      signalStateChange();
    }
  }

  public void markLocalFinished() {
    if (local != SideState.FINISHED) {
      local = SideState.FINISHED;
      signalStateChange();
    }
  }

  public void markRemoteEstablishedIfNotFinished() {
    if (remote == SideState.NEW) {
      remote = SideState.ESTABLISHED;
      signalStateChange();
    }
  }

  public void markRemoteFinished() {
    if (remote != SideState.FINISHED) {
      remote = SideState.FINISHED;
      signalStateChange();
    }
  }

  public void signalWaiters() {
    onStateChange.run();
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

  public boolean isCloseConverged() {
    return local == SideState.FINISHED && remote == SideState.FINISHED;
  }

  public boolean isClosing() {
    return localCloseRequested || local == SideState.FINISHED || remote == SideState.FINISHED;
  }

  private void signalStateChange() {
    onStateChange.run();
    if (isCloseConverged()) {
      onTerminal.run();
    }
  }
}
