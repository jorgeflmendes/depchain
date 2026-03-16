package pt.ulisboa.depchain.shared.network.links;

import java.util.ArrayDeque;
import java.util.Deque;

import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class DeliveryQueue<T> {
  private final Deque<T> queue = new ArrayDeque<>();
  private boolean closed;

  public synchronized void offer(T value) {
    ValidationUtils.requireNonNull(value, "value");
    if (closed) {
      return;
    }
    queue.addLast(value);
    notifyAll();
  }

  public synchronized T receive() throws InterruptedException {
    while (queue.isEmpty() && !closed) {
      wait();
    }
    if (queue.isEmpty()) {
      throw new LinkClosedException("Delivery queue is closed");
    }
    return queue.removeFirst();
  }

  public synchronized T receive(long timeoutMs) throws InterruptedException {
    long remainingMs = ValidationUtils.requireNonNegativeLong(timeoutMs, "timeoutMs");
    long deadlineNanos = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(remainingMs);

    while (queue.isEmpty() && !closed) {
      if (remainingMs <= 0L) {
        return null;
      }
      wait(remainingMs);
      remainingMs = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(Math.max(0L, deadlineNanos - System.nanoTime()));
    }

    if (queue.isEmpty()) {
      if (closed) {
        throw new LinkClosedException("Delivery queue is closed");
      }
      return null;
    }
    return queue.removeFirst();
  }

  public synchronized void close() {
    if (!closed) {
      closed = true;
      notifyAll();
    }
  }
}
