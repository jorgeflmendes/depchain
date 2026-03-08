package pt.ulisboa.depchain.shared.network.links.stubborn;

import java.io.IOException;
import java.net.InetSocketAddress;

import pt.ulisboa.depchain.shared.network.links.BlockingLink;
import pt.ulisboa.depchain.shared.network.links.LinkFailureException;
import pt.ulisboa.depchain.shared.network.links.fairloss.FairLossLink;
import pt.ulisboa.depchain.shared.network.links.fairloss.InboundBytes;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedKey;

public final class StubbornLink implements BlockingLink<InboundBytes> {
  // Default configs
  public static final int UNLIMITED_RETRY_ATTEMPTS = -1;
  public static final long DEFAULT_BASE_DELAY_MS = 80L;
  public static final long DEFAULT_MAX_DELAY_MS = 1_500L;
  public static final double DEFAULT_JITTER_RATIO = 0.20d;
  public static final int DEFAULT_MAX_RETRY_ATTEMPTS = 8;

  // Saves the shared state between the components.
  private final StubbornContext context;

  private final StubbornSender sender;
  private final StubbornReceiver receiver;

  // Handles the retry logic for tracked messages.
  private final StubbornRetryEngine retryEngine;

  // The thread running the retry loop.
  private final Thread retryLoopThread;

  private StubbornLink(FairLossLink fairLossLink) {
    this.context = new StubbornContext(fairLossLink);
    this.sender = new StubbornSender(context);
    this.receiver = new StubbornReceiver(context);
    this.retryEngine = new StubbornRetryEngine(context, sender);

    this.retryLoopThread = Thread.ofVirtual().name("stubborn-link").start(retryEngine::runRetryLoop);
  }

  public static StubbornLink bind(InetSocketAddress bindEndpoint) throws IOException {
    return new StubbornLink(FairLossLink.bind(bindEndpoint, FairLossLink.DEFAULT_MAX_PACKET_SIZE));
  }

  public static StubbornLink unbound() throws IOException {
    return new StubbornLink(FairLossLink.unbound(FairLossLink.DEFAULT_MAX_PACKET_SIZE));
  }

  // Sends a message without tracking, used for control messages that are not retried.
  public void send(byte[] payload, InetSocketAddress remoteEndpoint) throws IOException {
    sender.send(payload, remoteEndpoint);
  }

  // Sends a message and tracks it.
  public TrackedKey sendTracked(TrackedKey key, byte[] payload, InetSocketAddress remoteEndpoint) {
    return sender.sendTracked(key, payload, remoteEndpoint);
  }

  // Cancels a tracked message, preventing any future retries.
  public void cancelTracked(TrackedKey key, InetSocketAddress remoteEndpoint) {
    sender.cancelTracked(key, remoteEndpoint);
  }

  public LinkFailureException trackedFailureOrNull(TrackedKey key, InetSocketAddress remoteEndpoint) {
    return sender.trackedFailureOrNull(key, remoteEndpoint);
  }

  // Receives a message, blocking until one is available or the receiver is closed.
  @Override
  public InboundBytes receive() throws IOException {
    return receiver.receive();
  }

  // Receives a message with a timeout, returning null if the timeout expires or the receiver is
  // closed.
  @Override
  public InboundBytes receive(long timeoutMs) throws IOException {
    return receiver.receive(timeoutMs);
  }

  public long retryBudgetMs() {
    return context.retryBudgetMs();
  }

  @Override
  public void close() {
    if (!context.running.compareAndSet(true, false)) {
      return;
    }

    synchronized (context.stateLock) {
      context.stateLock.notifyAll();
    }

    retryLoopThread.interrupt();

    try {
      retryLoopThread.join(2_000L);
    } catch (InterruptedException ignored) {
      Thread.currentThread().interrupt();
    }

    context.fairLossLink.close();
  }
}
