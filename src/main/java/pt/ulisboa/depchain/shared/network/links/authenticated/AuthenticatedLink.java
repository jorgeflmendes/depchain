package pt.ulisboa.depchain.shared.network.links.authenticated;

import static pt.ulisboa.depchain.shared.validation.ValidationUtils.named;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.List;
import java.util.Map;

import javax.crypto.SecretKey;

import org.eclipse.jdt.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.ulisboa.depchain.proto.AuthOpcode;
import pt.ulisboa.depchain.proto.AuthenticatedDataEnvelope;
import pt.ulisboa.depchain.proto.AuthenticatedHandshakeEnvelope;
import pt.ulisboa.depchain.proto.DpchPacketType;
import pt.ulisboa.depchain.shared.crypto.CryptoUtil;
import pt.ulisboa.depchain.shared.crypto.key.PublicKeyLoader;
import pt.ulisboa.depchain.shared.network.links.BlockingLink;
import pt.ulisboa.depchain.shared.network.links.LinkClosedException;
import pt.ulisboa.depchain.shared.network.links.LinkThreadUtil;
import pt.ulisboa.depchain.shared.network.links.perfect.PerfectLink;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.validation.ValidationUtils;

public final class AuthenticatedLink implements BlockingLink<InboundPacket> {
  private static final Logger logger = LoggerFactory.getLogger(AuthenticatedLink.class);
  private static final long RECEIVE_MODE_RECHECK_MS = 50L;
  private final AuthenticatedContext context;
  private final AuthenticatedSender sender;
  private final Thread workerThread;

  private AuthenticatedLink(PerfectLink perfectLink, long localSenderId, PrivateKey localStaticSKey, Map<Long, PublicKey> staticPKeys) {
    this.context = new AuthenticatedContext(perfectLink, localSenderId, localStaticSKey, staticPKeys);
    this.sender = new AuthenticatedSender(context);
    this.workerThread = Thread.ofPlatform().daemon(true).name("authenticated-link").start(this::runInboundLoop);
  }

  public static AuthenticatedLink bind(InetSocketAddress bindEndpoint, long localSenderId, PrivateKey localStaticSKey, Map<Long, PublicKey> staticPKeys) throws IOException {
    ValidationUtils.requireAllNonNull(named("bindEndpoint", bindEndpoint), named("localStaticSKey", localStaticSKey), named("staticPKeys", staticPKeys));

    PerfectLink perfectLink = PerfectLink.bind(bindEndpoint);
    return new AuthenticatedLink(perfectLink, localSenderId, localStaticSKey, staticPKeys);
  }

  public static AuthenticatedLink unbound(long localSenderId, PrivateKey localStaticSKey, Map<Long, PublicKey> staticPKeys) throws IOException {
    ValidationUtils.requireAllNonNull(named("localStaticSKey", localStaticSKey), named("staticPKeys", staticPKeys));

    PerfectLink perfectLink = PerfectLink.unbound();
    return new AuthenticatedLink(perfectLink, localSenderId, localStaticSKey, staticPKeys);
  }

  public void send(long connectionId, byte[] payload, InetSocketAddress remoteEndpoint) {
    sender.send(connectionId, payload, remoteEndpoint);
  }

  @Override
  public InboundPacket receive() throws InterruptedException {
    while (context.isRunning()) {
      InboundPacket delivered = pollDelivered();
      if (delivered != null) {
        return delivered;
      }
      if (context.tryEnterDirectReceive()) {
        try {
          delivered = receiveDirectLoop(Long.MAX_VALUE);
          if (delivered != null) {
            return delivered;
          }
        } finally {
          context.exitDirectReceive();
        }
        continue;
      }

      delivered = context.receive(RECEIVE_MODE_RECHECK_MS);
      if (delivered != null) {
        return delivered;
      }
    }

    InboundPacket delivered = pollDelivered();
    if (delivered != null) {
      return delivered;
    }
    throw new LinkClosedException("AuthenticatedLink is closed");
  }

  @Override
  public @Nullable InboundPacket receive(long timeoutMs) throws InterruptedException {
    long deadlineNanos = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    while (context.isRunning()) {
      InboundPacket delivered = pollDelivered();
      if (delivered != null) {
        return delivered;
      }
      if (context.tryEnterDirectReceive()) {
        try {
          return receiveDirectLoop(deadlineNanos);
        } finally {
          context.exitDirectReceive();
        }
      }

      long remainingNanos = deadlineNanos - System.nanoTime();
      if (remainingNanos <= 0L) {
        return pollDelivered();
      }

      long waitMs = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(remainingNanos);
      if (waitMs > RECEIVE_MODE_RECHECK_MS) {
        waitMs = RECEIVE_MODE_RECHECK_MS;
      } else if (waitMs <= 0L) {
        waitMs = 1L;
      }

      delivered = context.receive(waitMs);
      if (delivered != null) {
        return delivered;
      }
    }

    return pollDelivered();
  }

  public void closeConnection(long connectionId, InetSocketAddress remoteEndpoint) {
    sender.closeConnection(connectionId, remoteEndpoint);
  }

  private void runInboundLoop() {
    while (context.isRunning()) {
      try {
        if (!context.awaitHandshakeWork()) {
          continue;
        }
        InboundPacket inbound = context.perfectLink.receive(50L);
        if (inbound == null) {
          continue;
        }
        InboundPacket packet = receivePacket(inbound);
        if (packet != null) {
          context.offer(packet);
        }
      } catch (LinkClosedException closed) {
        break;
      } catch (IllegalStateException exception) {
        if (!context.isRunning()) {
          break;
        }
        logger.debug("AuthenticatedLink worker error", exception);
      } catch (InterruptedException interrupted) {
        if (!context.isRunning()) {
          break;
        }
        Thread.currentThread().interrupt();
        break;
      } catch (Exception exception) {
        if (!context.isRunning()) {
          break;
        }
        logger.debug("AuthenticatedLink worker error", exception);
      }
    }
  }

  private @Nullable InboundPacket receiveDirect() throws InterruptedException {
    try {
      return receivePacket(context.perfectLink.receive());
    } catch (LinkClosedException closed) {
      throw closed;
    } catch (InterruptedException interrupted) {
      throw interrupted;
    } catch (Exception exception) {
      if (!context.isRunning()) {
        return null;
      }
      logger.debug("AuthenticatedLink direct receive error", exception);
      return null;
    }
  }

  private @Nullable InboundPacket receiveDirect(long timeoutMs) throws InterruptedException {
    try {
      InboundPacket inbound = context.perfectLink.receive(timeoutMs);
      if (inbound == null) {
        return null;
      }
      return receivePacket(inbound);
    } catch (LinkClosedException closed) {
      throw closed;
    } catch (InterruptedException interrupted) {
      throw interrupted;
    } catch (Exception exception) {
      if (!context.isRunning()) {
        return null;
      }
      logger.debug("AuthenticatedLink direct receive error", exception);
      return null;
    }
  }

  private @Nullable InboundPacket receiveDirectLoop(long deadlineNanos) throws InterruptedException {
    while (context.isRunning()) {
      InboundPacket directPacket;
      if (deadlineNanos == Long.MAX_VALUE) {
        directPacket = receiveDirect();
      } else {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0L) {
          return pollDelivered();
        }

        long remainingMs = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(remainingNanos);
        if (remainingMs <= 0L) {
          remainingMs = 1L;
        }
        directPacket = receiveDirect(remainingMs);
      }

      if (directPacket != null) {
        return directPacket;
      }

      InboundPacket delivered = pollDelivered();
      if (delivered != null) {
        return delivered;
      }
    }
    return pollDelivered();
  }

  private InboundPacket receivePacket(InboundPacket inbound) {
    if (inbound == null || inbound.packet().getPacketType() != DpchPacketType.DPCH_PACKET_TYPE_DATA) {
      return null;
    }

    ConnectionKey connectionKey = new ConnectionKey(inbound.sender(), inbound.packet().getConnectionId());
    AuthenticatedConnectionState connectionState = context.getConnectionStateOrNull(connectionKey);
    AuthenticatedHandshakeEnvelope handshakeEnvelope = decodeHandshakeEnvelope(inbound);
    if (handshakeEnvelope != null) {
      if (connectionState == null) {
        handleInit(connectionKey, inbound, handshakeEnvelope);
      } else {
        handleHandshake(connectionKey, connectionState, inbound, handshakeEnvelope);
      }
      return null;
    }

    if (connectionState == null) {
      sender.ensureHandshake(connectionKey, inbound.sender());
      return null;
    }

    return handleData(connectionState, inbound);
  }

  private void handleHandshake(ConnectionKey connectionKey, AuthenticatedConnectionState connectionState, InboundPacket inbound, AuthenticatedHandshakeEnvelope decodedPayload) {
    switch (connectionState.decideHandshake(decodedPayload.getAuthOpcode(), decodedPayload.getSenderId(), context.localSenderId)) {
      case USE_REPLY -> handleReply(connectionKey, inbound, decodedPayload);
      case RESTART -> handleInit(connectionKey, inbound, decodedPayload);
      case IGNORE -> {
      }
    }
  }

  private void handleInit(ConnectionKey connectionKey, InboundPacket inbound, AuthenticatedHandshakeEnvelope decodedPayload) {
    if (decodedPayload.getAuthOpcode() != AuthOpcode.AUTH_OPCODE_INIT) {
      return;
    }

    PublicKey senderStaticPublicKey = context.getStaticPublicKey(decodedPayload.getSenderId());
    if (senderStaticPublicKey == null) {
      return;
    }
    try {
      if (!AuthenticatedPayloadUtil.verifyEcdsa(decodedPayload, senderStaticPublicKey)) {
        return;
      }
    } catch (Exception ignored) {
      return;
    }

    PublicKey peerEphemeralPublicKey;
    try {
      peerEphemeralPublicKey = PublicKeyLoader.decodePublicKey(decodedPayload.getEphemeralPublicKeyBytes().toByteArray());
    } catch (Exception ignored) {
      return;
    }

    KeyPair localEphemeralKeys;
    try {
      localEphemeralKeys = CryptoUtil.createEcKeyPair();
    } catch (Exception exception) {
      throw new IllegalStateException("Failed to create ephemeral EC key pair", exception);
    }

    SecretKey sharedSecret;
    try {
      CryptoUtil.KeyContext keyContext = new CryptoUtil.KeyContext("SESSION_" + connectionKey.connectionId(), "HANDSHAKE");
      sharedSecret = CryptoUtil.deriveCommonKey(localEphemeralKeys.getPrivate(), peerEphemeralPublicKey, keyContext);
    } catch (Exception exception) {
      throw new IllegalStateException("Failed to derive shared secret", exception);
    }
    sender.sendReply(connectionKey, localEphemeralKeys, inbound.sender());
    AuthenticatedConnectionState connectionState = context.getOrCreateConnectionState(connectionKey);
    List<byte[]> queuedPayloads = connectionState.finishHandshake(sharedSecret, decodedPayload.getSenderId());
    sender.sendQueuedPayloads(connectionKey, connectionState, queuedPayloads, inbound.sender());
  }

  private void handleReply(ConnectionKey connectionKey, InboundPacket inbound, AuthenticatedHandshakeEnvelope decodedPayload) {
    if (decodedPayload.getAuthOpcode() != AuthOpcode.AUTH_OPCODE_REPLY) {
      return;
    }

    PublicKey responderStaticPublicKey = context.getStaticPublicKey(decodedPayload.getSenderId());
    if (responderStaticPublicKey == null) {
      return;
    }
    try {
      if (!AuthenticatedPayloadUtil.verifyEcdsa(decodedPayload, responderStaticPublicKey)) {
        return;
      }
    } catch (Exception ignored) {
      return;
    }

    PublicKey peerEphemeralPublicKey;
    try {
      peerEphemeralPublicKey = PublicKeyLoader.decodePublicKey(decodedPayload.getEphemeralPublicKeyBytes().toByteArray());
    } catch (Exception ignored) {
      return;
    }

    AuthenticatedConnectionState connectionState = context.getConnectionStateOrNull(connectionKey);
    if (connectionState == null) {
      return;
    }

    PrivateKey localEphemeralPrivateKey = connectionState.ephemeralPrivateKey();
    if (localEphemeralPrivateKey == null) {
      return;
    }

    SecretKey sharedSecret;
    try {
      CryptoUtil.KeyContext keyContext = new CryptoUtil.KeyContext("SESSION_" + connectionKey.connectionId(), "HANDSHAKE");
      sharedSecret = CryptoUtil.deriveCommonKey(localEphemeralPrivateKey, peerEphemeralPublicKey, keyContext);
    } catch (Exception exception) {
      throw new IllegalStateException("Failed to derive shared secret", exception);
    }
    if (connectionState.isAwaitingReply()) {
      context.signalAsyncHandshakeCompleted();
    }
    List<byte[]> queuedPayloads = connectionState.finishHandshake(sharedSecret, decodedPayload.getSenderId());
    sender.sendQueuedPayloads(connectionKey, connectionState, queuedPayloads, inbound.sender());
  }

  private InboundPacket handleData(AuthenticatedConnectionState connectionState, InboundPacket inbound) {
    AuthenticatedDataEnvelope decodedPayload;
    try {
      decodedPayload = AuthenticatedPayloadUtil.decodeHmac(inbound.payload());
    } catch (IllegalArgumentException ignored) {
      return null;
    }

    if (decodedPayload.getAuthOpcode() != AuthOpcode.AUTH_OPCODE_DATA) {
      return null;
    }

    boolean validHmac;
    try {
      validHmac = AuthenticatedPayloadUtil.verifyHmac(decodedPayload, connectionState.sharedSecret());
    } catch (Exception ignored) {
      validHmac = false;
    }
    if (!validHmac || !connectionState.validateAndIncrementReceivedNonce(decodedPayload.getNonce())) {
      return null;
    }

    return new InboundPacket(inbound.sender(), inbound.packet(), decodedPayload.getApplicationPayload(), connectionState.authenticatedRemoteSenderId());
  }

  private @Nullable InboundPacket pollDelivered() {
    return context.poll();
  }

  private @Nullable AuthenticatedHandshakeEnvelope decodeHandshakeEnvelope(InboundPacket inbound) {
    try {
      return AuthenticatedPayloadUtil.decodeEcdsa(inbound.payload());
    } catch (IllegalArgumentException ignored) {
      return null;
    }
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
      context.perfectLink.close();
    } finally {
      LinkThreadUtil.awaitStop(workerThread, "authenticated-link");
    }
  }
}
