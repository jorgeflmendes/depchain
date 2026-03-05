package pt.ulisboa.depchain.shared.network.links.stubborn;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import pt.ulisboa.depchain.shared.network.links.fairloss.FairLossLink;
import pt.ulisboa.depchain.shared.network.links.fairloss.InboundDatagram;
import pt.ulisboa.depchain.shared.network.links.stubborn.model.PendingRetry;
import pt.ulisboa.depchain.shared.network.links.stubborn.model.ScheduledRetry;
import pt.ulisboa.depchain.shared.network.links.stubborn.model.TrackedEndpointKey;
import pt.ulisboa.depchain.shared.network.links.stubborn.model.TrackedMessage;
import pt.ulisboa.depchain.shared.network.links.stubborn.policy.RetryPolicy;
import pt.ulisboa.depchain.shared.network.links.stubborn.registry.RetryRegistry;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class StubbornLink implements AutoCloseable {
  // Configuration parameters for the StubbornLink, with validation in the constructor.
  public record Config(long baseDelayMs, long maxDelayMs, double jitterRatio, int maxPending, int heapCompactMinSize, int maxRetryAttempts, long maxTrackedLifetimeMs) {
    public static final int UNLIMITED_RETRY_ATTEMPTS = -1;
    public static final long UNLIMITED_TRACKED_LIFETIME_MS = -1L;

    public Config {
      baseDelayMs = ValidationUtils.requirePositiveLong(baseDelayMs, "baseDelayMs");
      maxDelayMs = ValidationUtils.requirePositiveLong(maxDelayMs, "maxDelayMs");
      ValidationUtils.requireAtLeast(maxDelayMs, baseDelayMs, "maxDelayMs", "baseDelayMs");
      jitterRatio = ValidationUtils.requireInHalfOpenRangeDouble(jitterRatio, 0.0d, 1.0d, "jitterRatio");
      maxPending = ValidationUtils.requirePositiveInt(maxPending, "maxPending");
      heapCompactMinSize = ValidationUtils.requirePositiveInt(heapCompactMinSize, "heapCompactMinSize");
      if (maxRetryAttempts != UNLIMITED_RETRY_ATTEMPTS) {
        maxRetryAttempts = ValidationUtils.requirePositiveInt(maxRetryAttempts, "maxRetryAttempts");
      }
      if (maxTrackedLifetimeMs != UNLIMITED_TRACKED_LIFETIME_MS) {
        maxTrackedLifetimeMs = ValidationUtils.requirePositiveLong(maxTrackedLifetimeMs, "maxTrackedLifetimeMs");
      }
    }
  }

  private final FairLossLink fairLoss;

  // Components for managing tracked messages and their retries according to the configured retry policy.
  private final RetryPolicy retryPolicy;
  private final RetryRegistry retryRegistry;

  // Lock to protect access to the retry registry and coordinate the retry loop thread.
  private final Object stateLock = new Object();

  // Worker thread for managing retries, with a flag to signal when it should stop.
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final Thread retryLoopThread;

  private StubbornLink(FairLossLink fairLoss, Config config) {
    ValidationUtils.requireAllNonNull(named("fairLoss", fairLoss), named("config", config));
    this.fairLoss = fairLoss;
    this.retryPolicy = new RetryPolicy(config.baseDelayMs(), config.maxDelayMs(), config.jitterRatio(), config.maxRetryAttempts(), config.maxTrackedLifetimeMs());
    this.retryRegistry = new RetryRegistry(config.maxPending(), config.heapCompactMinSize(), Math.max(1024, config.maxPending() * 2), retryPolicy.pendingCancelTtlMs());
    this.retryLoopThread = Thread.ofVirtual().name("stubborn-link").start(this::runRetryLoop);
  }

  public static StubbornLink bind(InetAddress bindAddress, int port, int maxPacketSize, Config config) throws IOException {
    return new StubbornLink(FairLossLink.bind(bindAddress, port, maxPacketSize), config);
  }

  public static StubbornLink unbound(int maxPacketSize, Config config) throws IOException {
    return new StubbornLink(FairLossLink.unbound(maxPacketSize), config);
  }

  // Send without retries or tracking
  public void sendOnce(byte[] payload, InetAddress remoteIp, int remotePort) throws IOException {
    ensureOpen();
    ValidationUtils.requireAllNonNull(named("payload", payload), named("remoteIp", remoteIp));
    ValidationUtils.requireValidPort(remotePort, "remotePort");
    fairLoss.send(payload, remoteIp, remotePort);
  }

  // Send with retries and tracking (the returned key can be used to cancel pending retries if needed)
  public TrackedMessage.Key sendTracked(TrackedMessage.Key key, byte[] payload, InetAddress remoteIp, int remotePort) {
    ensureOpen();
    ValidationUtils.requireAllNonNull(named("key", key), named("payload", payload), named("remoteIp", remoteIp));
    ValidationUtils.requireValidPort(remotePort, "remotePort");

    InetSocketAddress endpoint = new InetSocketAddress(remoteIp, remotePort);
    long now = System.currentTimeMillis();
    TrackedMessage tracked;

    // Register the tracked message and its first retry attempt (if the max pending limit is reached, an exception is thrown).
    synchronized (stateLock) {
      ensureOpen();
      TrackedEndpointKey endpointKey = new TrackedEndpointKey(endpoint, key);
      if (retryRegistry.shouldSkipTrackedRegistration(endpointKey, now)) {
        return key;
      }
      if (!retryRegistry.canRegisterTracked(endpointKey)) {
        throw new IllegalStateException("StubbornLink rejected tracked send because maxPending was reached");
      }

      long nextRetryAtMs = now + retryPolicy.computeDelayMs(0);
      tracked = new TrackedMessage(key, payload, 0, now, nextRetryAtMs);
      retryRegistry.putTracked(endpointKey, tracked);
      stateLock.notifyAll();
    }

    sendIgnoringErrors(tracked.payload(), endpoint, tracked.key());
    return tracked.key();
  }

  // Cancel a tracked message (any pending retries will be removed and no further retries will be attempted)
  public void cancelTracked(TrackedMessage.Key key, InetAddress remoteIp, int remotePort) {
    ensureOpen();
    ValidationUtils.requireAllNonNull(named("key", key), named("remoteIp", remoteIp));
    ValidationUtils.requireValidPort(remotePort, "remotePort");

    long now = System.currentTimeMillis();
    synchronized (stateLock) {
      ensureOpen();
      TrackedEndpointKey endpointKey = new TrackedEndpointKey(new InetSocketAddress(remoteIp, remotePort), key);
      if (retryRegistry.removeTracked(endpointKey) == null) {
        retryRegistry.recordPendingCancel(endpointKey, now);
      }

      stateLock.notifyAll();
    }
  }

  // Receive the next delivered message, blocking until one is available.
  public InboundDatagram receive() throws IOException {
    ensureOpen();
    return fairLoss.receive();
  }

  public long baseDelayMs() {
    return retryPolicy.baseDelayMs();
  }

  public long maxDelayMs() {
    return retryPolicy.maxDelayMs();
  }

  public long maxTrackedLifetimeMs() {
    return retryPolicy.maxTrackedLifetimeMs();
  }

  private void runRetryLoop() {
    try {
      while (running.get()) {
        try {
          PendingRetry pendingSend = awaitNextRetrySend();
          if (pendingSend != null) {
            sendIgnoringErrors(pendingSend.payload(), pendingSend.endpoint(), pendingSend.key());
          }
        } catch (InterruptedException ignored) {
          if (!running.get()) {
            break;
          }
        } catch (RuntimeException exception) {
          System.out.printf("Stubborn retry loop error = %s%n", exception.getMessage());
        }
      }
    } finally {
      synchronized (stateLock) {
        retryRegistry.clear();
      }
    }
  }

  // Wait for the next pending retry to be due, returning its details (payload, endpoint, and key) or null if the link was closed while waiting.
  private PendingRetry awaitNextRetrySend() throws InterruptedException {
    synchronized (stateLock) {
      while (running.get()) {
        long now = System.currentTimeMillis();
        retryRegistry.prunePendingCancels(now);

        // Get the next scheduled retry and wait until it's due.
        ScheduledRetry scheduled = retryRegistry.peekScheduled();
        if (scheduled == null) {
          stateLock.wait();
          continue;
        }

        // Wait until it is due or until a new retry is scheduled (which could be sooner).
        long waitMs = scheduled.dueAtMs() - now;
        if (waitMs > 0L) {
          stateLock.wait(waitMs);
          continue;
        }

        // The next scheduled retry is due, so we poll it.
        retryRegistry.pollScheduled();
        TrackedEndpointKey endpointKey = new TrackedEndpointKey(scheduled.endpoint(), scheduled.messageKey());
        TrackedMessage tracked = retryRegistry.getTracked(endpointKey);
        if (tracked == null || tracked.nextRetryAtMs() != scheduled.dueAtMs()) { // The retry is no longer valid (it was cancelled or retried already), so we skip it and check the next one.
          continue;
        }

        // If the retry should no longer be tracked (because it reached the max retry attempts or max tracked lifetime), we remove it from the registry.
        if (retryPolicy.shouldStopTracking(tracked, now)) {
          retryRegistry.removeTracked(endpointKey);
          continue;
        }

        // Mark the retry attempt and compute the next retry time (if it needs further retries).
        tracked.markRetried(now + retryPolicy.computeDelayMs(tracked.retryAttempt() + 1));
        retryRegistry.putTracked(endpointKey, tracked);

        return new PendingRetry(tracked.payload(), tracked.key(), scheduled.endpoint());
      }
      return null;
    }
  }

  // Send a message ignoring any errors (used for retries, since failures are expected and will be handled by the retry logic)
  private void sendIgnoringErrors(byte[] payload, InetSocketAddress endpoint, TrackedMessage.Key key) {
    try {
      fairLoss.send(payload, endpoint.getAddress(), endpoint.getPort());
    } catch (IOException exception) {
      System.out.printf("Stubborn send error to %s:%d for conn=%s seq=%d tag=%d = %s%n", endpoint.getAddress().getHostAddress(), endpoint.getPort(), key.connectionId(), key.sequenceNumber(), key.messageTag(), exception.getMessage());
    }
  }

  private void ensureOpen() {
    if (!running.get()) {
      throw new IllegalStateException("StubbornLink is closed");
    }
  }

  @Override
  public void close() {
    if (!running.compareAndSet(true, false)) {
      return;
    }

    synchronized (stateLock) {
      stateLock.notifyAll();
    }

    retryLoopThread.interrupt();

    try {
      retryLoopThread.join(2_000L);
    } catch (InterruptedException ignored) {
      Thread.currentThread().interrupt();
    }

    fairLoss.close();
  }
}
