package pt.ulisboa.depchain.shared.network.links.perfect;

import java.io.IOException;
import java.net.InetSocketAddress;

import pt.ulisboa.depchain.shared.network.dpch.DpchType;
import pt.ulisboa.depchain.shared.network.links.BlockingLink;
import pt.ulisboa.depchain.shared.network.links.fairloss.FairLossLink;
import pt.ulisboa.depchain.shared.network.links.stubborn.StubbornLink;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;

public class PerfectLink implements BlockingLink<InboundPacket> {
  public static final int DEFAULT_MAX_WINDOW_SIZE = 1_000;
  public static final long DEFAULT_STREAM_IDLE_TTL_MS = 60_000L;
  public static final int DEFAULT_MAX_PACKET_SIZE = FairLossLink.DEFAULT_MAX_PACKET_SIZE;

  private final PerfectContext context;
  private final PerfectSender sender;
  private final PerfectReceiver receiver;
  private final Thread workerThread;

  public PerfectLink(StubbornLink stubbornLink, int maxWindowSize, long streamIdleTtlMs) {
    this.context = new PerfectContext(stubbornLink, maxWindowSize, streamIdleTtlMs);
    this.sender = new PerfectSender(context);
    this.receiver = new PerfectReceiver(context, sender);
    this.workerThread = Thread.ofVirtual().name("perfect-link").start(receiver::runInboundLoop);
  }

  public static PerfectLink bind(InetSocketAddress bindEndpoint) throws IOException {
    StubbornLink stubbornLink = StubbornLink.bind(bindEndpoint);
    return new PerfectLink(stubbornLink, DEFAULT_MAX_WINDOW_SIZE, DEFAULT_STREAM_IDLE_TTL_MS);
  }

  public static PerfectLink unbound() throws IOException {
    StubbornLink stubbornLink = StubbornLink.unbound();
    return new PerfectLink(stubbornLink, DEFAULT_MAX_WINDOW_SIZE, DEFAULT_STREAM_IDLE_TTL_MS);
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

  public long trackedNoPendingTimeoutMs() {
    return sender.trackedNoPendingTimeoutMs();
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
