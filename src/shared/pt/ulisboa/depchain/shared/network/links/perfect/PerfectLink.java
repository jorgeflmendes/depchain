package pt.ulisboa.depchain.shared.network.links.perfect;

import java.io.IOException;
import java.net.InetSocketAddress;

import pt.ulisboa.depchain.proto.DpchPacketType;
import pt.ulisboa.depchain.shared.network.links.BlockingLink;
import pt.ulisboa.depchain.shared.network.links.LinkThreadUtil;
import pt.ulisboa.depchain.shared.network.links.fairloss.FairLossLink;
import pt.ulisboa.depchain.shared.network.links.stubborn.StubbornLink;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;

public final class PerfectLink implements BlockingLink<InboundPacket> {
  public static final int MAX_PACKET_SIZE = FairLossLink.MAX_PACKET_SIZE;
  private static final byte[] EMPTY_CONTROL_PAYLOAD = new byte[0];

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

  public void sendData(long connectionId, byte[] payload, InetSocketAddress remoteEndpoint) {
    sender.send(connectionId, DpchPacketType.DPCH_PACKET_TYPE_DATA, false, payload, remoteEndpoint);
  }

  public void sendSyn(long connectionId, InetSocketAddress remoteEndpoint) {
    sender.send(connectionId, DpchPacketType.DPCH_PACKET_TYPE_SYN, false, EMPTY_CONTROL_PAYLOAD, remoteEndpoint);
  }

  public void sendSynAck(long connectionId, InetSocketAddress remoteEndpoint) {
    sender.send(connectionId, DpchPacketType.DPCH_PACKET_TYPE_SYN, true, EMPTY_CONTROL_PAYLOAD, remoteEndpoint);
  }

  public void sendFin(long connectionId, InetSocketAddress remoteEndpoint) {
    sender.send(connectionId, DpchPacketType.DPCH_PACKET_TYPE_FIN, false, EMPTY_CONTROL_PAYLOAD, remoteEndpoint);
  }

  public void sendFinAck(long connectionId, InetSocketAddress remoteEndpoint) {
    sender.send(connectionId, DpchPacketType.DPCH_PACKET_TYPE_FIN, true, EMPTY_CONTROL_PAYLOAD, remoteEndpoint);
  }

  public void sendAck(long connectionId, int acknowledgedSequence, DpchPacketType acknowledgedType, InetSocketAddress remoteEndpoint) {
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

  public boolean awaitNoPendingSyn(long connectionId, InetSocketAddress remoteEndpoint, long timeoutMs) throws InterruptedException {
    return sender.awaitNoPendingType(connectionId, remoteEndpoint, DpchPacketType.DPCH_PACKET_TYPE_SYN, timeoutMs);
  }

  public boolean awaitNoPendingFin(long connectionId, InetSocketAddress remoteEndpoint, long timeoutMs) throws InterruptedException {
    return sender.awaitNoPendingType(connectionId, remoteEndpoint, DpchPacketType.DPCH_PACKET_TYPE_FIN, timeoutMs);
  }

  public boolean awaitNoPendingData(long connectionId, InetSocketAddress remoteEndpoint, long timeoutMs) throws InterruptedException {
    return sender.awaitNoPendingType(connectionId, remoteEndpoint, DpchPacketType.DPCH_PACKET_TYPE_DATA, timeoutMs);
  }

  public void cancelPendingData(long connectionId, InetSocketAddress remoteEndpoint) {
    sender.cancelPendingType(connectionId, remoteEndpoint, DpchPacketType.DPCH_PACKET_TYPE_DATA);
  }

  public void cancelPendingControl(long connectionId, InetSocketAddress remoteEndpoint) {
    sender.cancelPendingType(connectionId, remoteEndpoint, DpchPacketType.DPCH_PACKET_TYPE_SYN);
    sender.cancelPendingType(connectionId, remoteEndpoint, DpchPacketType.DPCH_PACKET_TYPE_FIN);
  }

  public void releaseConnection(long connectionId, InetSocketAddress remoteEndpoint) {
    context.releaseConnection(connectionId, remoteEndpoint);
  }

  @Override
  public void close() throws Exception {
    if (!context.stop()) {
      return;
    }

    try {
      context.shutdown();
      workerThread.interrupt();
      context.stubbornLink.close();
    } finally {
      LinkThreadUtil.awaitStop(workerThread, "perfect-link");
    }
  }
}

