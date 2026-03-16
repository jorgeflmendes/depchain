package pt.ulisboa.depchain.shared.network.links.handshaked;

import java.io.IOException;
import java.net.InetSocketAddress;

import pt.ulisboa.depchain.shared.network.links.BlockingLink;
import pt.ulisboa.depchain.shared.network.links.LinkThreadUtil;
import pt.ulisboa.depchain.shared.network.links.perfect.PerfectLink;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;

public final class HandshakedPerfectLink implements BlockingLink<InboundPacket> {
  private final HandshakedContext context;
  private final HandshakedSender sender;
  private final HandshakedReceiver receiver;
  private final Thread workerThread;

  private HandshakedPerfectLink(PerfectLink perfectLink) {
    this.context = new HandshakedContext(perfectLink);
    this.sender = new HandshakedSender(context);
    this.receiver = new HandshakedReceiver(context, sender);

    this.workerThread = Thread.ofVirtual().name("handshaked-perfect-link").start(receiver::runInboundLoop);
  }

  public static HandshakedPerfectLink bind(InetSocketAddress bindEndpoint) throws IOException {
    PerfectLink perfect = PerfectLink.bind(bindEndpoint);
    return new HandshakedPerfectLink(perfect);
  }

  public static HandshakedPerfectLink unbound() throws IOException {
    PerfectLink perfect = PerfectLink.unbound();
    return new HandshakedPerfectLink(perfect);
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
    if (!context.stop()) {
      return;
    }
    try {
      context.shutdown();
      workerThread.interrupt();
      context.perfectLink.close();
    } finally {
      LinkThreadUtil.awaitStop(workerThread, "handshaked-perfect-link");
    }
  }
}
