package pt.ulisboa.depchain.shared.network.links.authenticated;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Map;

import org.eclipse.jdt.annotation.Nullable;

import pt.ulisboa.depchain.shared.network.links.BlockingLink;
import pt.ulisboa.depchain.shared.network.links.LinkThreadUtil;
import pt.ulisboa.depchain.shared.network.links.handshaked.HandshakedPerfectLink;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class AuthenticatedLink implements BlockingLink<InboundPacket> {
  private final AuthenticatedContext context;
  private final AuthenticatedSender sender;
  private final AuthenticatedReceiver receiver;
  private final Thread workerThread;

  private AuthenticatedLink(HandshakedPerfectLink handshakedLink, long localSenderId, PrivateKey localStaticSKey, Map<Long, PublicKey> staticPKeys) {
    this.context = new AuthenticatedContext(handshakedLink, localSenderId, localStaticSKey, staticPKeys);
    this.sender = new AuthenticatedSender(context);
    this.receiver = new AuthenticatedReceiver(context, sender);
    this.workerThread = Thread.ofVirtual().name("authenticated-link").start(receiver::runInboundLoop);
  }

  public static AuthenticatedLink bind(InetSocketAddress bindEndpoint) throws IOException {
    throw new UnsupportedOperationException("AuthenticatedLink.bind requires local senderId, local static private key, and peer static public keys");
  }

  public static AuthenticatedLink bind(InetSocketAddress bindEndpoint, long localSenderId, PrivateKey localStaticSKey, Map<Long, PublicKey> staticPKeys) throws IOException {
    ValidationUtils.requireAllNonNull(named("bindEndpoint", bindEndpoint), named("localStaticSKey", localStaticSKey), named("staticPKeys", staticPKeys));

    HandshakedPerfectLink handshaked = HandshakedPerfectLink.bind(bindEndpoint);
    return new AuthenticatedLink(handshaked, localSenderId, localStaticSKey, staticPKeys);
  }

  public static AuthenticatedLink unbound() throws IOException {
    throw new UnsupportedOperationException("AuthenticatedLink.unbound requires local senderId, local static private key, and peer static public keys");
  }

  public static AuthenticatedLink unbound(long localSenderId, PrivateKey localStaticSKey, Map<Long, PublicKey> staticPKeys) throws IOException {
    ValidationUtils.requireAllNonNull(named("localStaticSKey", localStaticSKey), named("staticPKeys", staticPKeys));

    HandshakedPerfectLink handshaked = HandshakedPerfectLink.unbound();
    return new AuthenticatedLink(handshaked, localSenderId, localStaticSKey, staticPKeys);
  }

  public void send(long connectionId, byte[] payload, InetSocketAddress remoteEndpoint) {
    sender.send(connectionId, payload, remoteEndpoint);
  }

  @Override
  public InboundPacket receive() throws InterruptedException {
    return context.receive();
  }

  @Override
  public @Nullable InboundPacket receive(long timeoutMs) throws InterruptedException {
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
      context.closeAllConnectionStates();
      context.handshakedLink.close();
    } finally {
      LinkThreadUtil.awaitStop(workerThread, "authenticated-link");
    }
  }
}
