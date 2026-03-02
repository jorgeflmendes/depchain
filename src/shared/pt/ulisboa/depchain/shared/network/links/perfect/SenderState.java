package pt.ulisboa.depchain.shared.network.links.perfect;

import java.util.concurrent.atomic.AtomicInteger;

final class SenderState {
  // The next sequence number to be sent
  private final AtomicInteger nextSequence = new AtomicInteger(0);

  // Timestamp of the last activity on this stream, used for staleness checks
  private volatile long lastTouchedAtMs = System.currentTimeMillis();

  int nextSequence(long now) {
    touch(now);
    return nextSequence.getAndIncrement();
  }

  void touch(long now) {
    lastTouchedAtMs = now;
  }

  // If it has not been touched for a while.
  boolean isStale(long now, long ttlMs) {
    return (now - lastTouchedAtMs) >= ttlMs;
  }
}
