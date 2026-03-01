package pt.ulisboa.depchain.shared.network.links.stubborn;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import pt.ulisboa.depchain.shared.network.links.fairloss.FairLossLink;
import pt.ulisboa.depchain.shared.network.messages.InboundMessage;
import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class StubbornLink implements AutoCloseable {
  // Transport dependency.
  private final FairLossLink fairLoss;

  // Retry configuration.
  private final long baseDelayMs;
  private final long maxDelayMs;
  private final double jitterRatio;
  private final int maxPending;
  private final int minRetryHeapSizeForCompaction;

  // Retry state (owned by event loop thread):
  private final Map<EndpointTrackedKey, TrackedMessage> activeTrackedByEndpointKey; // Active tracked messages indexed by endpoint+key.
  private final Set<EndpointTrackedKey> cancelTombstones; // Tombstones for cancel requests that arrived before the corresponding tracked send was registered.
  private final PriorityQueue<ScheduledRetry> retryScheduleHeap; // May contain stale entries

  // Event-loop coordination.
  private final BlockingQueue<Event> eventQueue;
  private final Thread eventLoopThread;

  // Lifecycle synchronization.
  private final AtomicBoolean running;
  private final Object lock;

  // Build one stubborn link on top of fair-loss transport.
  private StubbornLink(FairLossLink fairLoss, long baseDelayMs, long maxDelayMs, double jitterRatio, int maxPending, int minRetryHeapSizeForCompaction) {
    this.fairLoss = Objects.requireNonNull(fairLoss, "fairLoss cannot be null");
    this.baseDelayMs = ValidationUtils.requirePositiveLong(baseDelayMs, "baseDelayMs");
    this.maxDelayMs = ValidationUtils.requirePositiveLong(maxDelayMs, "maxDelayMs");

    if (this.maxDelayMs < this.baseDelayMs) {
      throw new IllegalArgumentException("maxDelayMs must be >= baseDelayMs");
    }
    
    if (jitterRatio < 0.0d || jitterRatio >= 1.0d) {
      throw new IllegalArgumentException("jitterRatio must be in [0.0, 1.0)");
    }
    this.jitterRatio = jitterRatio;
    
    this.maxPending = ValidationUtils.requirePositiveInt(maxPending, "maxPending");
    this.minRetryHeapSizeForCompaction = ValidationUtils.requirePositiveInt(minRetryHeapSizeForCompaction, "heapCompactMinSize");
    
    this.lock = new Object();
    this.activeTrackedByEndpointKey = new HashMap<>();
    this.cancelTombstones = new HashSet<>();
    this.retryScheduleHeap = new PriorityQueue<>(Comparator.comparingLong(ScheduledRetry::dueAtMs));
    this.eventQueue = new LinkedBlockingQueue<>();
    this.running = new AtomicBoolean(true);
    this.eventLoopThread = Thread.ofVirtual().name("stubborn-link-loop").start(this::runEventLoop);
  }

  // Bind stubborn link to a local address/port.
  public static StubbornLink bind(InetAddress bindIp, int port, int maxPacketSize, long baseDelayMs, long maxDelayMs, double jitterRatio, int maxPending, int minRetryHeapSizeForCompaction) throws IOException {
    FairLossLink fairLoss = FairLossLink.bind(bindIp, port, maxPacketSize);
    return new StubbornLink(fairLoss, baseDelayMs, maxDelayMs, jitterRatio, maxPending, minRetryHeapSizeForCompaction);
  }

  // Create stubborn link with ephemeral local socket (client mode).
  public static StubbornLink unbound(int maxPacketSize, long baseDelayMs, long maxDelayMs, double jitterRatio, int maxPending, int minRetryHeapSizeForCompaction) throws IOException {
    FairLossLink fairLoss = FairLossLink.unbound(maxPacketSize);
    return new StubbornLink(fairLoss, baseDelayMs, maxDelayMs, jitterRatio, maxPending, minRetryHeapSizeForCompaction);
  }

  // Send once through fair-loss without tracking.
  public void sendOnce(Dpch packet, InetAddress targetIp, int targetPort) throws IOException {
    ensureOpen();
    Objects.requireNonNull(packet, "packet cannot be null");
    Objects.requireNonNull(targetIp, "targetIp cannot be null");
    ValidationUtils.requireValidPort(targetPort, "targetPort");
    fairLoss.send(packet, targetIp, targetPort);
  }

  // Send now and start retry tracking for this message key.
  public TrackedMessage.Key sendTracked(Dpch packet, InetAddress targetIp, int targetPort) {
    Objects.requireNonNull(packet, "packet cannot be null");
    Objects.requireNonNull(targetIp, "targetIp cannot be null");
    ValidationUtils.requireValidPort(targetPort, "targetPort");

    InetSocketAddress endpoint = new InetSocketAddress(targetIp, targetPort);
    TrackedMessage.Key key = TrackedMessage.Key.fromPacket(packet);
    enqueueEvent(new SendTrackedEvent(endpoint, key, packet));
    return key;
  }

  // Force immediate resend and re-schedule of a tracked key.
  public void forceResend(TrackedMessage.Key key, InetAddress targetIp, int targetPort) {
    Objects.requireNonNull(key, "key cannot be null");
    Objects.requireNonNull(targetIp, "targetIp cannot be null");
    ValidationUtils.requireValidPort(targetPort, "targetPort");
    
    enqueueEvent(new ForceResendEvent(new InetSocketAddress(targetIp, targetPort), key));
  }

  // Stop retries for a tracked key at one destination.
  public void cancelTracked(TrackedMessage.Key key, InetAddress targetIp, int targetPort) {
    Objects.requireNonNull(key, "key cannot be null");
    Objects.requireNonNull(targetIp, "targetIp cannot be null");
    ValidationUtils.requireValidPort(targetPort, "targetPort");

    enqueueEvent(new CancelTrackedEvent(new InetSocketAddress(targetIp, targetPort), key));
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
            handleSendTracked(sendTracked);
          } else if (event instanceof CancelTrackedEvent cancelTracked) {
            handleCancelTracked(cancelTracked);
          } else if (event instanceof ForceResendEvent forceResend) {
            handleForceResend(forceResend);
          }
        } catch (Exception exception) {
          System.out.printf("Stubborn event loop error = %s%n", exception.getMessage());
        }
      }
    } finally {
      activeTrackedByEndpointKey.clear();
      cancelTombstones.clear();
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
  private void handleSendTracked(SendTrackedEvent event) throws IOException {
    long now = System.currentTimeMillis();
    sendIgnoringErrors(event.packet(), event.endpoint());
    EndpointTrackedKey endpointTrackedKey = new EndpointTrackedKey(event.endpoint(), event.key());
    if (cancelTombstones.remove(endpointTrackedKey)) {
      return;
    }

    TrackedMessage previous = activeTrackedByEndpointKey.get(endpointTrackedKey);
    if (previous == null && activeTrackedByEndpointKey.size() >= maxPending) {
      return; // Drop, limit exceeded.
    }

    // Start tracking with initial retry deadline.
    long nextRetryAtMs = now + computeDelayMs(0);
    TrackedMessage tracked = new TrackedMessage(event.key(), event.packet(), 0, now, nextRetryAtMs);
    activeTrackedByEndpointKey.put(endpointTrackedKey, tracked);
    scheduleRetry(event.endpoint(), tracked);
  }

  // Remove tracked message state for endpoint+key.
  private void handleCancelTracked(CancelTrackedEvent event) {
    EndpointTrackedKey endpointTrackedKey = new EndpointTrackedKey(event.endpoint(), event.key());
    TrackedMessage removed = activeTrackedByEndpointKey.remove(endpointTrackedKey);
    if (removed == null) {
      recordPendingCancel(endpointTrackedKey);
    }
  }

  // Resend now and push retry deadline forward.
  private void handleForceResend(ForceResendEvent event) throws IOException {
    TrackedMessage tracked = activeTrackedByEndpointKey.get(new EndpointTrackedKey(event.endpoint(), event.key()));
    if (tracked == null) {
      return;
    }

    resendAndSchedule(event.endpoint(), tracked, System.currentTimeMillis());
  }

  // Retry every message whose deadline is due.
  private void processDueRetries(long now) throws IOException {
    while (true) {
      ScheduledRetry scheduled = retryScheduleHeap.peek();
      if (scheduled == null || scheduled.dueAtMs() > now) {
        return;
      }
      retryScheduleHeap.poll();

      TrackedMessage tracked = activeTrackedByEndpointKey.get(new EndpointTrackedKey(scheduled.endpoint(), scheduled.messageKey()));
      if (tracked == null) {
        continue;
      }

      if (tracked.nextRetryAtMs() != scheduled.dueAtMs()) {
        continue;
      }

      resendAndSchedule(scheduled.endpoint(), tracked, now);
    }
  }

  // Best-effort send that keeps retry state even if send fails.
  private void sendIgnoringErrors(Dpch packet, InetSocketAddress endpoint) throws IOException {
    fairLoss.send(packet, endpoint.getAddress(), endpoint.getPort());
  }

  // Resend one tracked message now and enqueue its next retry.
  private void resendAndSchedule(InetSocketAddress endpoint, TrackedMessage tracked, long now) throws IOException {
    sendIgnoringErrors(tracked.packet(), endpoint);
    tracked.markRetried(now + computeDelayMs(tracked.retryAttempt() + 1));
    scheduleRetry(endpoint, tracked);
  }

  // Enqueue the next retry for one tracked message.
  private void scheduleRetry(InetSocketAddress endpoint, TrackedMessage tracked) {
    retryScheduleHeap.offer(new ScheduledRetry(endpoint, tracked.key(), tracked.nextRetryAtMs()));
    compactRetryHeapIfNeeded();
  }

  // Record a cancel tombstone for an endpoint+key, to be applied if a tracked send arrives later with the same key.
  private void recordPendingCancel(EndpointTrackedKey endpointTrackedKey) {
    cancelTombstones.add(endpointTrackedKey);
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

  // Reject API calls after close.
  private void enqueueEvent(Event event) {
    synchronized (lock) {
      ensureOpen();
      eventQueue.offer(event);
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
