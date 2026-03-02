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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import pt.ulisboa.depchain.shared.network.dpch.DpchSerialization;
import pt.ulisboa.depchain.shared.network.links.fairloss.FairLossLink;
import pt.ulisboa.depchain.shared.network.links.fairloss.InboundDatagram;
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

  // Retry state (guarded by stateLock):
  private final Map<EndpointTrackedKey, TrackedMessage> activeTrackedByEndpointKey; // active tracked messages indexed by endpoint+key.
  private final Map<EndpointTrackedKey, Long> pendingCancels; // arrived before the corresponding tracked send was registered.
  private final PriorityQueue<ScheduledRetry> retryScheduleHeap; // May contain stale entries

  // Retry-loop coordination.
  private final Object stateLock;
  private final Thread retryLoopThread;

  // Lifecycle synchronization.
  private final AtomicBoolean running;

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

    this.stateLock = new Object();
    this.activeTrackedByEndpointKey = new HashMap<>();
    this.pendingCancels = new HashMap<>();
    this.retryScheduleHeap = new PriorityQueue<>(Comparator.comparingLong(ScheduledRetry::dueAtMs));
    this.running = new AtomicBoolean(true);

    this.retryLoopThread = Thread.ofVirtual().name("stubborn-link").start(this::runRetryLoop);
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
    fairLoss.send(DpchSerialization.toBytes(packet), remoteIp, remotePort);
  }

  // Send now and start retry tracking for this message key.
  public TrackedMessage.Key sendTracked(Dpch packet, InetAddress remoteIp, int remotePort) {
    ensureOpen();
    Objects.requireNonNull(packet, "packet cannot be null");
    Objects.requireNonNull(remoteIp, "remoteIp cannot be null");
    ValidationUtils.requireValidPort(remotePort, "remotePort");

    InetSocketAddress endpoint = new InetSocketAddress(remoteIp, remotePort);
    TrackedMessage.Key key = TrackedMessage.Key.fromPacket(packet);
    long now = System.currentTimeMillis();

    synchronized (stateLock) {
      ensureOpen();
      EndpointTrackedKey endpointTrackedKey = new EndpointTrackedKey(endpoint, key);
      Long cancelAtMs = pendingCancels.remove(endpointTrackedKey);
      if (cancelAtMs != null && (now - cancelAtMs) <= maxTrackedLifetimeMs) {
        return key;
      }

      TrackedMessage previous = activeTrackedByEndpointKey.get(endpointTrackedKey);
      if (previous == null && activeTrackedByEndpointKey.size() >= maxPending) {
        throw new IllegalStateException("StubbornLink rejected tracked send because maxPending was reached");
      }

      // Start tracking before first send so transient send failures still keep retry state.
      long nextRetryAtMs = now + computeDelayMs(0);
      TrackedMessage tracked = new TrackedMessage(key, packet, 0, now, nextRetryAtMs);
      activeTrackedByEndpointKey.put(endpointTrackedKey, tracked);
      scheduleRetryLocked(endpoint, tracked);
      stateLock.notifyAll();
    }

    sendIgnoringErrors(packet, endpoint);
    return key;
  }

  // Stop retries for a tracked key at one destination.
  public void cancelTracked(TrackedMessage.Key key, InetAddress remoteIp, int remotePort) {
    ensureOpen();
    Objects.requireNonNull(key, "key cannot be null");
    Objects.requireNonNull(remoteIp, "remoteIp cannot be null");
    ValidationUtils.requireValidPort(remotePort, "remotePort");

    long now = System.currentTimeMillis();
    synchronized (stateLock) {
      ensureOpen();
      EndpointTrackedKey endpointTrackedKey = new EndpointTrackedKey(new InetSocketAddress(remoteIp, remotePort), key);
      TrackedMessage removed = activeTrackedByEndpointKey.remove(endpointTrackedKey);
      if (removed == null) {
        recordPendingCancelLocked(endpointTrackedKey, now);
      }
      stateLock.notifyAll();
    }
  }

  // Receive one inbound packet from fair-loss.
  public InboundMessage receive() throws IOException {
    ensureOpen();
    InboundDatagram datagram = fairLoss.receive();
    byte[] payload = datagram.payload();
    Dpch decoded = DpchSerialization.fromBytes(payload, 0, payload.length);
    return new InboundMessage(decoded, datagram.senderIp(), datagram.senderPort());
  }

  // Retry loop that processes due retransmissions.
  private void runRetryLoop() {
    try {
      while (running.get()) {
        try {
          PendingRetrySend pendingRetrySend = null;

          synchronized (stateLock) {
            while (running.get()) {
              long now = System.currentTimeMillis();
              if (!pendingCancels.isEmpty()) {
                prunePendingCancelsLocked(now);
              }

              ScheduledRetry scheduled = retryScheduleHeap.peek();
              if (scheduled == null) {
                stateLock.wait();
                continue;
              }

              long waitMs = scheduled.dueAtMs() - now;
              if (waitMs > 0L) {
                stateLock.wait(waitMs);
                continue;
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

              tracked.markRetried(now + computeDelayMs(tracked.retryAttempt() + 1));
              scheduleRetryLocked(scheduled.endpoint(), tracked);
              pendingRetrySend = new PendingRetrySend(tracked.packet(), scheduled.endpoint());
              break;
            }
          }

          if (pendingRetrySend != null) {
            sendIgnoringErrors(pendingRetrySend.packet(), pendingRetrySend.endpoint());
          }
        } catch (InterruptedException ignored) {
          if (!running.get()) {
            break;
          }
        } catch (Exception exception) {
          System.out.printf("Stubborn retry loop error = %s%n", exception.getMessage());
        }
      }
    } finally {
      synchronized (stateLock) {
        activeTrackedByEndpointKey.clear();
        pendingCancels.clear();
        retryScheduleHeap.clear();
      }
    }
  }

  // Best-effort send that keeps retry state even if send fails.
  private void sendIgnoringErrors(Dpch packet, InetSocketAddress endpoint) {
    try {
      fairLoss.send(DpchSerialization.toBytes(packet), endpoint.getAddress(), endpoint.getPort());
    } catch (IOException exception) {
      System.out.printf("Stubborn send error to %s:%d for conn=%s seq=%d type=%s = %s%n", endpoint.getAddress().getHostAddress(), endpoint.getPort(), packet.connectionId(), packet.sequenceNumber(), packet.type(), exception.getMessage());
    }
  }

  // Enqueue the next retry for one tracked message.
  private void scheduleRetryLocked(InetSocketAddress endpoint, TrackedMessage tracked) {
    retryScheduleHeap.offer(new ScheduledRetry(endpoint, tracked.key(), tracked.nextRetryAtMs()));
    compactRetryHeapIfNeededLocked();
  }

  // Record one pending cancel for an endpoint+key, to be applied if a tracked send arrives later with the same key.
  private void recordPendingCancelLocked(EndpointTrackedKey endpointTrackedKey, long now) {
    if (pendingCancels.size() >= maxPendingCancels) {
      prunePendingCancelsLocked(now);
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
  private void prunePendingCancelsLocked(long now) {
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

  // Check that the link is still open, throwing if it has been closed.
  private void ensureOpen() {
    if (!running.get()) {
      throw new IllegalStateException("StubbornLink is closed");
    }
  }

  // Rebuild heap when stale scheduled retries significantly outnumber active ones.
  private void compactRetryHeapIfNeededLocked() {
    int heapSize = retryScheduleHeap.size();
    if (heapSize < minRetryHeapSizeForCompaction) {
      return;
    }

    int activeTrackedSize = activeTrackedByEndpointKey.size();
    int staleEstimate = heapSize - activeTrackedSize;
    if (staleEstimate <= activeTrackedSize) {
      return;
    }

    rebuildRetryHeapLocked();
  }

  // Rebuild the retry heap from pending tracked messages, removing stale entries.
  private void rebuildRetryHeapLocked() {
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

  // Compound key for tracked state indexed by endpoint + tracked message key.
  private record EndpointTrackedKey(InetSocketAddress endpoint, TrackedMessage.Key key) {
    private EndpointTrackedKey {
      Objects.requireNonNull(endpoint, "endpoint cannot be null");
      Objects.requireNonNull(key, "key cannot be null");
    }
  }

  private record PendingRetrySend(Dpch packet, InetSocketAddress endpoint) {
    private PendingRetrySend {
      Objects.requireNonNull(packet, "packet cannot be null");
      Objects.requireNonNull(endpoint, "endpoint cannot be null");
    }
  }
}
