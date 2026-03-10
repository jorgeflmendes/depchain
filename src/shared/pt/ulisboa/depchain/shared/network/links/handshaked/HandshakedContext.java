package pt.ulisboa.depchain.shared.network.links.handshaked;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import pt.ulisboa.depchain.shared.network.links.handshaked.registry.ConnectionStateRegistry;
import pt.ulisboa.depchain.shared.network.links.perfect.PerfectLink;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class HandshakedContext {
  final PerfectLink perfectLink;

  final AtomicBoolean running = new AtomicBoolean(true);

  final ConnectionStateRegistry connectionStateRegistry;
  final BlockingQueue<InboundPacket> deliveryQueue = new LinkedBlockingQueue<>();

  HandshakedContext(PerfectLink perfectLink) {
    this.perfectLink = ValidationUtils.requireNonNull(perfectLink, "perfectLink");
    this.connectionStateRegistry = new ConnectionStateRegistry();
  }

  InboundPacket receive() throws InterruptedException {
    return deliveryQueue.take();
  }

  InboundPacket receive(long timeoutMs) throws InterruptedException {
    return deliveryQueue.poll(ValidationUtils.requireNonNegativeLong(timeoutMs, "timeoutMs"), TimeUnit.MILLISECONDS);
  }
}
