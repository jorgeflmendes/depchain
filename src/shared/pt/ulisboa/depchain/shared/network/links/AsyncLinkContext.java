package pt.ulisboa.depchain.shared.network.links;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AsyncLinkContext<T> {
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final DeliveryQueue<T> deliveryQueue = new DeliveryQueue<>();

  public final boolean isRunning() {
    return running.get();
  }

  public final boolean stop() {
    return running.compareAndSet(true, false);
  }

  public final T receive() throws InterruptedException {
    return deliveryQueue.receive();
  }

  public final T receive(long timeoutMs) throws InterruptedException {
    return deliveryQueue.receive(timeoutMs);
  }

  public final void offer(T value) {
    deliveryQueue.offer(value);
  }

  protected final void shutdownInbox() {
    deliveryQueue.close();
  }
}
