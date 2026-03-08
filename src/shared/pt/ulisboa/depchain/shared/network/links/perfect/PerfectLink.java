package pt.ulisboa.depchain.shared.network.links.perfect;

import java.io.IOException;
import java.net.InetSocketAddress;

import pt.ulisboa.depchain.shared.network.dpch.DpchType;
import pt.ulisboa.depchain.shared.network.links.BlockingLink;
import pt.ulisboa.depchain.shared.network.links.LinkFailureException;
import pt.ulisboa.depchain.shared.network.links.fairloss.FairLossLink;
import pt.ulisboa.depchain.shared.network.links.stubborn.StubbornLink;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;

public class PerfectLink implements BlockingLink<InboundPacket> {
  public static final int DEFAULT_MAX_PACKET_SIZE = FairLossLink.DEFAULT_MAX_PACKET_SIZE;

  private final PerfectContext context;
  private final PerfectSender sender;
  private final PerfectReceiver receiver;
  private final Thread workerThread;

  public PerfectLink(StubbornLink stubbornLink) {
    this.context = new PerfectContext(stubbornLink);
    this.sender = new PerfectSender(context);
    this.receiver = new PerfectReceiver(context, sender);
    this.workerThread = Thread.ofVirtual().name("perfect-link").start(receiver::runInboundLoop);
  }

  public static PerfectLink bind(InetSocketAddress bindEndpoint) throws IOException {
    StubbornLink stubbornLink = StubbornLink.bind(bindEndpoint);
    return new PerfectLink(stubbornLink);
  }

  public static PerfectLink unbound() throws IOException {
    StubbornLink stubbornLink = StubbornLink.unbound();
    return new PerfectLink(stubbornLink);
  }

  public void send(long connectionId, DpchType packetType, boolean withAck, byte[] payload, InetSocketAddress remoteEndpoint) {
    sender.send(connectionId, packetType, withAck, payload, remoteEndpoint);
  }

  public void sendAck(long connectionId, int acknowledgedSequence, DpchType acknowledgedType, InetSocketAddress remoteEndpoint) {
    sender.sendAck(connectionId, acknowledgedSequence, acknowledgedType, remoteEndpoint);
  }

  @Override
  public InboundPacket receive() throws InterruptedException {
    return context.receive();
  }

  @Override
  public InboundPacket receive(long timeoutMs) throws InterruptedException {
    return context.receive(timeoutMs);
  }

  public boolean waitUntilNoPendingData(long connectionId, InetSocketAddress remoteEndpoint, long timeoutMs) throws InterruptedException {
    return sender.waitUntilNoPendingData(connectionId, remoteEndpoint, timeoutMs);
  }

  public boolean waitUntilNoPendingType(long connectionId, InetSocketAddress remoteEndpoint, DpchType packetType, long timeoutMs) throws InterruptedException {
    return sender.waitUntilNoPendingType(connectionId, remoteEndpoint, packetType, timeoutMs);
  }

  public void cancelPendingType(long connectionId, InetSocketAddress remoteEndpoint, DpchType packetType) {
    sender.cancelPendingType(connectionId, remoteEndpoint, packetType);
  }

  public void throwIfTrackedFailed(long connectionId, InetSocketAddress remoteEndpoint, DpchType packetType) throws LinkFailureException {
    context.throwIfTrackedFailed(connectionId, remoteEndpoint, packetType);
  }

  @Override
  public void close() throws Exception {
    if (!context.running.compareAndSet(true, false)) {
      return;
    }

    try {
      context.stubbornLink.close();
    } finally {
      workerThread.interrupt();
      workerThread.join(2_000L);
    }
  }
}
