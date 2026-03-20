package pt.ulisboa.depchain.shared.network.links.fairloss;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.jdt.annotation.Nullable;

import pt.ulisboa.depchain.shared.network.links.BlockingLink;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;

public final class FairLossLink extends SimpleChannelInboundHandler<DatagramPacket> implements BlockingLink<InboundBytes> {
  public static final int MAX_PACKET_SIZE = 8_192;

  private static final long RECEIVE_POLL_MS = 100L;

  private final MultiThreadIoEventLoopGroup ioGroup;
  private final BlockingQueue<InboundBytes> receivedPackets;
  private final AtomicBoolean closed;
  private volatile @Nullable IOException transportFailure;
  private Channel channel;

  private FairLossLink(MultiThreadIoEventLoopGroup ioGroup) {
    ValidationUtils.requireNonNull(ioGroup, "ioGroup");

    this.ioGroup = ioGroup;
    this.receivedPackets = new LinkedBlockingQueue<>();
    this.closed = new AtomicBoolean(false);
  }

  public static FairLossLink bind(InetSocketAddress bindEndpoint) throws IOException {
    ValidationUtils.requireNonNull(bindEndpoint, "bindEndpoint");
    return open(bindEndpoint);
  }

  public static FairLossLink unbound() throws IOException {
    return open(new InetSocketAddress(0));
  }

  public void send(byte[] payload, InetSocketAddress remoteEndpoint) throws IOException {
    ValidationUtils.requireAllNonNull(named("payload", payload), named("remoteEndpoint", remoteEndpoint));
    ValidationUtils.requireNonNull(remoteEndpoint.getAddress(), "remoteEndpoint.address");
    ValidationUtils.requireValidPort(remoteEndpoint.getPort(), "remoteEndpoint.port");
    if (payload.length > MAX_PACKET_SIZE) {
      throw new IllegalArgumentException("Serialized payload exceeds MAX_PACKET_SIZE (%d > %d)".formatted(payload.length, MAX_PACKET_SIZE));
    }
    ensureUsable();

    DatagramPacket datagram = new DatagramPacket(Unpooled.copiedBuffer(payload), remoteEndpoint);
    ChannelFuture sendFuture = channel.writeAndFlush(datagram).syncUninterruptibly();
    if (!sendFuture.isSuccess()) {
      throw new IOException("Failed to send datagram", sendFuture.cause());
    }
  }

  @Override
  public InboundBytes receive() throws IOException {
    while (true) {
      InboundBytes inbound;
      try {
        inbound = receivedPackets.poll(RECEIVE_POLL_MS, TimeUnit.MILLISECONDS);
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while waiting for datagram", exception);
      }

      if (inbound == null) {
        ensureUsable();
        continue;
      }
      return inbound;
    }
  }

  @Override
  public @Nullable InboundBytes receive(long timeoutMs) throws IOException {
    ValidationUtils.requireNonNegativeLong(timeoutMs, "timeoutMs");

    InboundBytes inbound;
    try {
      inbound = receivedPackets.poll(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while waiting for datagram", exception);
    }

    if (inbound == null) {
      ensureUsable();
      return null;
    }
    return inbound;
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    if (channel != null) {
      channel.close().syncUninterruptibly();
    }
    ioGroup.shutdownGracefully().syncUninterruptibly();
  }

  private static FairLossLink open(InetSocketAddress bindEndpoint) throws IOException {
    MultiThreadIoEventLoopGroup ioGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    FairLossLink link = new FairLossLink(ioGroup);
    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(ioGroup);
    bootstrap.channel(NioDatagramChannel.class);
    bootstrap.option(ChannelOption.SO_BROADCAST, false);
    bootstrap.handler(link);

    try {
      ChannelFuture bindFuture = bootstrap.bind(bindEndpoint).syncUninterruptibly();
      if (!bindFuture.isSuccess()) {
        throw new IOException("Failed to open UDP link on " + bindEndpoint, bindFuture.cause());
      }
      link.channel = bindFuture.channel();
      return link;
    } catch (IOException | RuntimeException exception) {
      ioGroup.shutdownGracefully().syncUninterruptibly();
      if (exception instanceof IOException ioException) {
        throw ioException;
      }
      throw new IOException("Failed to open UDP link on " + bindEndpoint, exception);
    }
  }

  private void ensureUsable() throws IOException {
    IOException failure = transportFailure;
    if (failure != null) {
      throw failure;
    }
    if (closed.get() || channel == null || !channel.isOpen()) {
      throw new IOException("UDP link is closed");
    }
  }

  @Override
  protected void channelRead0(ChannelHandlerContext context, DatagramPacket packet) {
    ByteBuf content = packet.content();
    byte[] payload = new byte[content.readableBytes()];
    content.getBytes(content.readerIndex(), payload);
    receivedPackets.offer(new InboundBytes((InetSocketAddress) packet.sender(), payload));
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
    if (transportFailure == null) {
      if (cause instanceof IOException ioException) {
        transportFailure = ioException;
      } else {
        transportFailure = new IOException("Netty UDP transport failure", cause);
      }
    }
    context.close();
  }
}
