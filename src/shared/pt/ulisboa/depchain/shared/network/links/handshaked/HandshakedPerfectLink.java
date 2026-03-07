package pt.ulisboa.depchain.shared.network.links.handshaked;

import java.io.IOException;
import java.net.InetSocketAddress;

import pt.ulisboa.depchain.shared.network.links.BlockingLink;
import pt.ulisboa.depchain.shared.network.links.perfect.PerfectLink;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;

public final class HandshakedPerfectLink implements BlockingLink<InboundPacket> {
  private static final long DEFAULT_CONNECTION_IDLE_TTL_MS = PerfectLink.DEFAULT_STREAM_IDLE_TTL_MS;

  private final HandshakedContext context;
  private final HandshakedSender sender;
  private final HandshakedReceiver receiver;
  private final Thread workerThread;

  private HandshakedPerfectLink(PerfectLink perfectLink, long connectionIdleTtlMs) {
    this.context = new HandshakedContext(perfectLink, connectionIdleTtlMs);
    this.sender = new HandshakedSender(context, connectionIdleTtlMs);
    this.receiver = new HandshakedReceiver(context, sender);
    
    this.workerThread = Thread.ofVirtual().name("handshaked-perfect-link").start(receiver::runInboundLoop);
  }

  public static HandshakedPerfectLink bind(InetSocketAddress bindEndpoint) throws IOException {
    PerfectLink perfect = PerfectLink.bind(bindEndpoint);
    return new HandshakedPerfectLink(perfect, DEFAULT_CONNECTION_IDLE_TTL_MS);
  }

  public static HandshakedPerfectLink unbound() throws IOException {
    PerfectLink perfect = PerfectLink.unbound();
    return new HandshakedPerfectLink(perfect, DEFAULT_CONNECTION_IDLE_TTL_MS);
  }

  public void send(long connectionId, byte[] payload, InetSocketAddress remoteEndpoint) {
    sender.send(connectionId, payload, remoteEndpoint);
  }

  @Override
  public InboundPacket receive() throws InterruptedException {
    return context.receive();
  }

  @Override
  public InboundPacket receive(long timeoutMs) throws InterruptedException {
    return context.receive(timeoutMs);
  }

  public void closeConnection(long connectionId, InetSocketAddress remoteEndpoint) {
    sender.closeConnection(connectionId, remoteEndpoint);
  }

  @Override
  public void close() throws Exception {
    if (!context.running.compareAndSet(true, false)) {
      return;
    }
    try {
      context.perfectLink.close();
    } finally {
      workerThread.interrupt();
      try {
        workerThread.join(2_000L);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
