package pt.ulisboa.depchain.shared.network.links.stubborn;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import pt.ulisboa.depchain.shared.network.links.fairloss.FairLossLink;
import pt.ulisboa.depchain.shared.network.model.InboundMessage;
import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class StubbornLink implements AutoCloseable {
  // Retry and tracking configuration.
  public record Config(long baseDelayMs, long maxDelayMs, double jitterRatio, int maxPending, int heapCompactMinSize, int maxRetryAttempts, long maxTrackedLifetimeMs) {
    public Config {
      baseDelayMs = ValidationUtils.requirePositiveLong(baseDelayMs, "baseDelayMs");
      maxDelayMs = ValidationUtils.requirePositiveLong(maxDelayMs, "maxDelayMs");
      if (maxDelayMs < baseDelayMs) {
        throw new IllegalArgumentException("maxDelayMs must be >= baseDelayMs");
      }
      if (jitterRatio < 0.0d || jitterRatio >= 1.0d) {
        throw new IllegalArgumentException("jitterRatio must be in [0.0, 1.0)");
      }
      maxPending = ValidationUtils.requirePositiveInt(maxPending, "maxPending");
      heapCompactMinSize = ValidationUtils.requirePositiveInt(heapCompactMinSize, "heapCompactMinSize");
      maxRetryAttempts = ValidationUtils.requirePositiveInt(maxRetryAttempts, "maxRetryAttempts");
      maxTrackedLifetimeMs = ValidationUtils.requirePositiveLong(maxTrackedLifetimeMs, "maxTrackedLifetimeMs");
    }
  }

  // Transport dependency.
  private final FairLossLink fairLoss;

  // Retry configuration.
  private final long baseDelayMs;
  private final long maxDelayMs;
  private final double jitterRatio;
  private final int maxPending;
  private final int minRetryHeapSizeForCompaction;
  private final int maxRetryAttempts;
  private final long maxTrackedLifetimeMs;
  private final int maxPendingCancels;

  // Retry state (owned by event loop thread):
  private final Map<EndpointTrackedKey, TrackedMessage> activeTrackedByEndpointKey; // active tracked messages indexed by endpoint+key.
  private final Map<EndpointTrackedKey, Long> pendingCancels; // arrived before the corresponding tracked send was registered.
  private final PriorityQueue<ScheduledRetry> retryScheduleHeap; // May contain stale entries

  // Event-loop coordination.
  private final BlockingQueue<Event> eventQueue;
  private final Thread eventLoopThread;

  // Lifecycle synchronization.
  private final AtomicBoolean running;
  private final Object lock;

  // Build one stubborn link on top of fair-loss transport.
  private StubbornLink(FairLossLink fairLoss, Config config) {
    this.fairLoss = Objects.requireNonNull(fairLoss, "fairLoss cannot be null");
    Objects.requireNonNull(config, "config cannot be null");
    this.baseDelayMs = config.baseDelayMs();
    this.maxDelayMs = config.maxDelayMs();
    this.jitterRatio = config.jitterRatio();
    this.maxPending = config.maxPending();
    this.minRetryHeapSizeForCompaction = config.heapCompactMinSize();
    this.maxRetryAttempts = config.maxRetryAttempts();
    this.maxTrackedLifetimeMs = config.maxTrackedLifetimeMs();
    this.maxPendingCancels = Math.max(1024, this.maxPending * 2);
    
    this.lock = new Object();
    this.activeTrackedByEndpointKey = new HashMap<>();
    this.pendingCancels = new HashMap<>();
    this.retryScheduleHeap = new PriorityQueue<>(Comparator.comparingLong(ScheduledRetry::dueAtMs));
    this.eventQueue = new LinkedBlockingQueue<>();
    this.running = new AtomicBoolean(true);

    this.eventLoopThread = Thread.ofVirtual().name("stubborn-link").start(this::runEventLoop);
  }

  // Bind stubborn link to a local address/port.
  public static StubbornLink bind(InetAddress bindAddress, int port, int maxPacketSize, Config config) throws IOException {
    FairLossLink fairLoss = FairLossLink.bind(bindAddress, port, maxPacketSize);
    return new StubbornLink(fairLoss, config);
  }

  // Create stubborn link with ephemeral local socket (client mode).
  public static StubbornLink unbound(int maxPacketSize, Config config) throws IOException {
    FairLossLink fairLoss = FairLossLink.unbound(maxPacketSize);
    return new StubbornLink(fairLoss, config);
  }

  // Send once through fair-loss without tracking.
  public void sendOnce(Dpch packet, InetAddress remoteIp, int remotePort) throws IOException {
    ensureOpen();
    Objects.requireNonNull(packet, "packet cannot be null");
    Objects.requireNonNull(remoteIp, "remoteIp cannot be null");
    ValidationUtils.requireValidPort(remotePort, "remotePort");
    fairLoss.send(packet, remoteIp, remotePort);
  }

  // Send now and start retry tracking for this message key.
  public TrackedMessage.Key sendTracked(Dpch packet, InetAddress remoteIp, int remotePort) {
    Objects.requireNonNull(packet, "packet cannot be null");
    Objects.requireNonNull(remoteIp, "remoteIp cannot be null");
    ValidationUtils.requireValidPort(remotePort, "remotePort");

    InetSocketAddress endpoint = new InetSocketAddress(remoteIp, remotePort);
    TrackedMessage.Key key = TrackedMessage.Key.fromPacket(packet);
    CompletableFuture<Boolean> accepted = new CompletableFuture<>();

    enqueueEvent(new SendTrackedEvent(endpoint, key, packet, accepted));
    if (!awaitTrackedRegistration(accepted)) {
      throw new IllegalStateException("StubbornLink rejected tracked send because maxPending was reached");
    }
    return key;
  }

  // Stop retries for a tracked key at one destination.
  public void cancelTracked(TrackedMessage.Key key, InetAddress remoteIp, int remotePort) {
    Objects.requireNonNull(key, "key cannot be null");
    Objects.requireNonNull(remoteIp, "remoteIp cannot be null");
    ValidationUtils.requireValidPort(remotePort, "remotePort");

    enqueueEvent(new CancelTrackedEvent(new InetSocketAddress(remoteIp, remotePort), key));
  }

  // Receive one inbound packet from fair-loss.
  public InboundMessage receive() throws IOException {
    ensureOpen();
    return fairLoss.receive();
  }

  // Event loop that processes retries and control commands.
  private void runEventLoop() {
    try {
      while (running.get()) {
        try {
          long now = System.currentTimeMillis();
          processDueRetries(now);
          if (!pendingCancels.isEmpty()) {
            prunePendingCancels(now);
          }

          // Wait for next control event until the nearest retry deadline.
          long timeoutMs = millisUntilNextRetry(now);
          Event event = pollEvent(timeoutMs);
          if (event == null) {
            continue;
          }

          if (event instanceof ShutdownEvent) {
            break;
          }

          if (event instanceof SendTrackedEvent sendTracked) {
            boolean accepted = false;
            try {
              accepted = handleSendTracked(sendTracked);
            } finally {
              sendTracked.accepted().complete(accepted);
            }
          } else if (event instanceof CancelTrackedEvent cancelTracked) {
            handleCancelTracked(cancelTracked);
          }
        } catch (Exception exception) {
          System.out.printf("Stubborn event loop error = %s%n", exception.getMessage());
        }
      }
    } finally {
      activeTrackedByEndpointKey.clear();
      pendingCancels.clear();
      retryScheduleHeap.clear();
    }
  }

  // Wait for next control event until the nearest retry deadline.
  private Event pollEvent(long timeoutMs) {
    try {
      if (timeoutMs == Long.MAX_VALUE) {
        return eventQueue.take(); // No retries scheduled, wait indefinitely for next event.
      }

      if (timeoutMs <= 0L) {
        return eventQueue.poll(); // Retry deadline is due, don't wait for next event to process retries on time.
      }

      return eventQueue.poll(timeoutMs, TimeUnit.MILLISECONDS); // Wait for next event or retry deadline, whichever comes first.
    } catch (InterruptedException ignored) {
      return null;
    }
  }

  // Register/update tracked message state and schedule first retry.
  private boolean handleSendTracked(SendTrackedEvent event) {
    long now = System.currentTimeMillis();
    EndpointTrackedKey endpointTrackedKey = new EndpointTrackedKey(event.endpoint(), event.key());
    Long cancelAtMs = pendingCancels.remove(endpointTrackedKey);
    if (cancelAtMs != null && (now - cancelAtMs) <= maxTrackedLifetimeMs) {
      return true;
    }

    TrackedMessage previous = activeTrackedByEndpointKey.get(endpointTrackedKey);
    if (previous == null && activeTrackedByEndpointKey.size() >= maxPending) {
      return false; // Reject, limit exceeded.
    }

    // Start tracking before first send so transient send failures still keep retry state.
    long nextRetryAtMs = now + computeDelayMs(0);
    TrackedMessage tracked = new TrackedMessage(event.key(), event.packet(), 0, now, nextRetryAtMs);
    activeTrackedByEndpointKey.put(endpointTrackedKey, tracked);
    sendIgnoringErrors(event.packet(), event.endpoint());
    scheduleRetry(event.endpoint(), tracked);
    return true;
  }

  // Remove tracked message state for endpoint+key.
  private void handleCancelTracked(CancelTrackedEvent event) {
    long now = System.currentTimeMillis();
    EndpointTrackedKey endpointTrackedKey = new EndpointTrackedKey(event.endpoint(), event.key());
    TrackedMessage removed = activeTrackedByEndpointKey.remove(endpointTrackedKey);
    if (removed == null) {
      recordPendingCancel(endpointTrackedKey, now);
    }
  }

  // Retry every message whose deadline is due.
  private void processDueRetries(long now) {
    while (true) {
      ScheduledRetry scheduled = retryScheduleHeap.peek();
      if (scheduled == null || scheduled.dueAtMs() > now) {
        return;
      }
      retryScheduleHeap.poll();

      EndpointTrackedKey endpointTrackedKey = new EndpointTrackedKey(scheduled.endpoint(), scheduled.messageKey());
      TrackedMessage tracked = activeTrackedByEndpointKey.get(endpointTrackedKey);
      if (tracked == null) {
        continue;
      }

      if (tracked.nextRetryAtMs() != scheduled.dueAtMs()) {
        continue;
      }

      if (shouldStopTracking(tracked, now)) {
        activeTrackedByEndpointKey.remove(endpointTrackedKey);
        continue;
      }

      resendAndSchedule(scheduled.endpoint(), tracked, now);
    }
  }

  // Best-effort send that keeps retry state even if send fails.
  private void sendIgnoringErrors(Dpch packet, InetSocketAddress endpoint) {
    try {
      fairLoss.send(packet, endpoint.getAddress(), endpoint.getPort());
    } catch (IOException exception) {
      System.out.printf("Stubborn send error to %s:%d for conn=%d seq=%d type=%s = %s%n", endpoint.getAddress().getHostAddress(), endpoint.getPort(), packet.connectionId(), packet.sequenceNumber(), packet.type(), exception.getMessage());
    }
  }

  // Resend one tracked message now and enqueue its next retry.
  private void resendAndSchedule(InetSocketAddress endpoint, TrackedMessage tracked, long now) {
    sendIgnoringErrors(tracked.packet(), endpoint);
    tracked.markRetried(now + computeDelayMs(tracked.retryAttempt() + 1));
    scheduleRetry(endpoint, tracked);
  }

  // Enqueue the next retry for one tracked message.
  private void scheduleRetry(InetSocketAddress endpoint, TrackedMessage tracked) {
    retryScheduleHeap.offer(new ScheduledRetry(endpoint, tracked.key(), tracked.nextRetryAtMs()));
    compactRetryHeapIfNeeded();
  }

  // Record one pending cancel for an endpoint+key, to be applied if a tracked send arrives later with the same key.
  private void recordPendingCancel(EndpointTrackedKey endpointTrackedKey, long now) {
    if (pendingCancels.size() >= maxPendingCancels) {
      prunePendingCancels(now);
    }

    if (pendingCancels.size() >= maxPendingCancels) {
      Iterator<EndpointTrackedKey> iterator = pendingCancels.keySet().iterator();
      if (iterator.hasNext()) {
        iterator.next();
        iterator.remove();
      }
    }

    pendingCancels.put(endpointTrackedKey, now);
  }

  // Drop expired pending cancels and keep map size bounded.
  private void prunePendingCancels(long now) {
    pendingCancels.entrySet().removeIf(entry -> (now - entry.getValue()) >= maxTrackedLifetimeMs);
    if (pendingCancels.size() <= maxPendingCancels) {
      return;
    }

    Iterator<EndpointTrackedKey> iterator = pendingCancels.keySet().iterator();
    while (pendingCancels.size() > maxPendingCancels && iterator.hasNext()) {
      iterator.next();
      iterator.remove();
    }
  }

  // Time until next retry deadline.
  private long millisUntilNextRetry(long now) {
    ScheduledRetry next = retryScheduleHeap.peek();
    if (next == null) {
      return Long.MAX_VALUE;
    }
    long delta = next.dueAtMs() - now;
    return Math.max(0L, delta);
  }

  // Exponential backoff with jitter.
  private long computeDelayMs(int attempt) {
    double exp = baseDelayMs * Math.pow(2.0d, attempt);
    double capped = Math.min(exp, maxDelayMs);
    double jitter = (jitterRatio == 0.0d) ? 0.0d : ThreadLocalRandom.current().nextDouble(-jitterRatio, jitterRatio);
    long delay = Math.round(capped * (1.0d + jitter));
    return Math.max(1L, delay);
  }

  // Decide if one tracked message should stop retrying due to configured limits.
  private boolean shouldStopTracking(TrackedMessage tracked, long now) {
    if (tracked.retryAttempt() >= maxRetryAttempts) {
      return true;
    }

    long ageMs = now - tracked.createdAtMs();
    return ageMs >= maxTrackedLifetimeMs;
  }

  private void enqueueEvent(Event event) {
    synchronized (lock) {
      ensureOpen();
      eventQueue.offer(event);
    }
  }

  // Wait for event-loop acknowledgement so tracked-send admission failures are visible to caller.
  private boolean awaitTrackedRegistration(CompletableFuture<Boolean> accepted) {
    try {
      return accepted.get(2, TimeUnit.SECONDS);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for tracked send registration", interrupted);
    } catch (ExecutionException executionException) {
      throw new IllegalStateException("Failed to register tracked send", executionException.getCause());
    } catch (TimeoutException timeoutException) {
      throw new IllegalStateException("Timed out while waiting for tracked send registration", timeoutException);
    }
  }

  // Check that the link is still open, throwing if it has been closed.
  private void ensureOpen() {
    if (!running.get()) {
      throw new IllegalStateException("StubbornLink is closed");
    }
  }

  // Rebuild heap when stale scheduled retries significantly outnumber active ones.
  private void compactRetryHeapIfNeeded() {
    int heapSize = retryScheduleHeap.size();
    if (heapSize < minRetryHeapSizeForCompaction) {
      return;
    }

    int activeTrackedSize = activeTrackedByEndpointKey.size();
    int staleEstimate = heapSize - activeTrackedSize;
    if (staleEstimate <= activeTrackedSize) {
      return;
    }

    rebuildRetryHeap();
  }

  // Rebuild the retry heap from pending tracked messages, removing stale entries.
  private void rebuildRetryHeap() {
    PriorityQueue<ScheduledRetry> rebuilt = new PriorityQueue<>(Comparator.comparingLong(ScheduledRetry::dueAtMs));
    for (Map.Entry<EndpointTrackedKey, TrackedMessage> entry : activeTrackedByEndpointKey.entrySet()) {
      EndpointTrackedKey endpointTrackedKey = entry.getKey();
      TrackedMessage tracked = entry.getValue();
      rebuilt.offer(new ScheduledRetry(endpointTrackedKey.endpoint(), tracked.key(), tracked.nextRetryAtMs()));
    }

    retryScheduleHeap.clear();
    retryScheduleHeap.addAll(rebuilt);
  }

  // Stop retries and close underlying fair-loss link.
  @Override
  public void close() {
    synchronized (lock) {
      if (!running.compareAndSet(true, false)) {
        return;
      }
      eventQueue.offer(new ShutdownEvent());
    }
    eventLoopThread.interrupt();

    try {
      eventLoopThread.join(2_000L);
    } catch (InterruptedException ignored) {
      Thread.currentThread().interrupt();
    }

    fairLoss.close();
  }

  // Compound key for tracked state indexed by endpoint + tracked message key.
  private record EndpointTrackedKey(InetSocketAddress endpoint, TrackedMessage.Key key) {
    private EndpointTrackedKey {
      Objects.requireNonNull(endpoint, "endpoint cannot be null");
      Objects.requireNonNull(key, "key cannot be null");
    }
  }
}
