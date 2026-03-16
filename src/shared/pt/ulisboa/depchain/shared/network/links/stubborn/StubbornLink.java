package pt.ulisboa.depchain.shared.network.links.stubborn;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.eclipse.jdt.annotation.Nullable;

import pt.ulisboa.depchain.shared.network.links.BlockingLink;
import pt.ulisboa.depchain.shared.network.links.LinkFailureException;
import pt.ulisboa.depchain.shared.network.links.LinkThreadUtil;
import pt.ulisboa.depchain.shared.network.links.fairloss.FairLossLink;
import pt.ulisboa.depchain.shared.network.links.fairloss.InboundBytes;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedKey;

public final class StubbornLink implements BlockingLink<InboundBytes> {
  public static final long DEFAULT_BASE_DELAY_MS = 80L;
  public static final long DEFAULT_MAX_DELAY_MS = 1_500L;
  public static final double DEFAULT_JITTER_RATIO = 0.20d;
  public static final int DEFAULT_MAX_RETRY_ATTEMPTS = 8;

  private final StubbornContext context;
  private final StubbornSender sender;
  private final StubbornReceiver receiver;
  private final StubbornRetryEngine retryEngine;
  private final Thread retryLoopThread;

  private StubbornLink(FairLossLink fairLossLink) {
    this.context = new StubbornContext(fairLossLink);
    this.sender = new StubbornSender(context);
    this.receiver = new StubbornReceiver(context);
    this.retryEngine = new StubbornRetryEngine(context, sender);

    this.retryLoopThread = Thread.ofVirtual().name("stubborn-link").start(retryEngine::runRetryLoop);
  }

  public static StubbornLink bind(InetSocketAddress bindEndpoint) throws IOException {
    return new StubbornLink(FairLossLink.bind(bindEndpoint));
  }

  public static StubbornLink unbound() throws IOException {
    return new StubbornLink(FairLossLink.unbound());
  }

  public void send(byte[] payload, InetSocketAddress remoteEndpoint) throws IOException {
    sender.send(payload, remoteEndpoint);
  }

  public TrackedKey sendTracked(TrackedKey key, byte[] payload, InetSocketAddress remoteEndpoint) {
    return sendTrackedWithTerminalNotification(key, payload, remoteEndpoint, () -> {
    });
  }

  public TrackedKey sendTrackedWithTerminalNotification(TrackedKey key, byte[] payload, InetSocketAddress remoteEndpoint, Runnable onTerminalState) {
    return sender.sendTrackedWithTerminalNotification(key, payload, remoteEndpoint, onTerminalState);
  }

  public void cancelTracked(TrackedKey key, InetSocketAddress remoteEndpoint) {
    sender.cancelTracked(key, remoteEndpoint);
  }

  public LinkFailureException pollTerminalFailure(TrackedKey key, InetSocketAddress remoteEndpoint) {
    return sender.pollTerminalFailure(key, remoteEndpoint);
  }

  @Override
  public InboundBytes receive() throws IOException {
    return receiver.receive();
  }

  @Override
  public @Nullable InboundBytes receive(long timeoutMs) throws IOException {
    return receiver.receive(timeoutMs);
  }

  @Override
  public void close() {
    if (!context.running.compareAndSet(true, false)) {
      return;
    }

    synchronized (context.stateLock) {
      context.stateLock.notifyAll();
    }

    context.fairLossLink.close();
    retryLoopThread.interrupt();
    LinkThreadUtil.awaitStop(retryLoopThread, "stubborn-link");
  }
}
