package pt.ulisboa.depchain.shared.network.links.authenticated;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

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
import pt.ulisboa.depchain.proto.DpchPacket;
import pt.ulisboa.depchain.proto.DpchPacketType;
import pt.ulisboa.depchain.shared.keys.PublicKeyLoader;
import pt.ulisboa.depchain.shared.network.links.BlockingLink;
import pt.ulisboa.depchain.shared.network.links.LinkClosedException;
import pt.ulisboa.depchain.shared.network.links.LinkThreadUtil;
import pt.ulisboa.depchain.shared.network.links.handshaked.HandshakedPerfectLink;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class AuthenticatedLink implements BlockingLink<InboundPacket> {
  private static final Logger logger = LoggerFactory.getLogger(AuthenticatedLink.class);
  private final AuthenticatedContext context;
  private final AuthenticatedSender sender;
  private final Thread workerThread;

  private AuthenticatedLink(HandshakedPerfectLink handshakedLink, long localSenderId, PrivateKey localStaticSKey, Map<Long, PublicKey> staticPKeys) {
    this.context = new AuthenticatedContext(handshakedLink, localSenderId, localStaticSKey, staticPKeys);
    this.sender = new AuthenticatedSender(context);
    this.workerThread = Thread.ofVirtual().name("authenticated-link").start(this::runInboundLoop);
  }

  public static AuthenticatedLink bind(InetSocketAddress bindEndpoint, long localSenderId, PrivateKey localStaticSKey, Map<Long, PublicKey> staticPKeys) throws IOException {
    ValidationUtils.requireAllNonNull(named("bindEndpoint", bindEndpoint), named("localStaticSKey", localStaticSKey), named("staticPKeys", staticPKeys));

    HandshakedPerfectLink handshaked = HandshakedPerfectLink.bind(bindEndpoint);
    return new AuthenticatedLink(handshaked, localSenderId, localStaticSKey, staticPKeys);
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

  private void runInboundLoop() {
    while (context.isRunning()) {
      try {
        InboundPacket inbound = context.handshakedLink.receive();
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
      }
    }
  }

  private InboundPacket receivePacket(InboundPacket inbound) {
    if (inbound == null) {
      return null;
    }

    ConnectionKey connectionKey = new ConnectionKey(inbound.sender(), inbound.packet().getConnectionId());
    AuthenticatedConnectionState connectionState = context.getConnectionStateOrNull(connectionKey);
    if (connectionState != null && connectionState.receiveMode() == AuthenticatedConnectionState.ReceiveMode.DATA) {
      return handleData(connectionState, inbound);
    }
    if (connectionState != null && connectionState.receiveMode() == AuthenticatedConnectionState.ReceiveMode.HANDSHAKE) {
      return handleHandshake(connectionKey, connectionState, inbound);
    }

    if (inbound.packet().getPacketType() != DpchPacketType.DPCH_PACKET_TYPE_DATA) {
      return null;
    }

    AuthenticatedHandshakeEnvelope decodedPayload;
    try {
      decodedPayload = AuthenticatedPayloadUtil.decodeEcdsa(inbound.packet().getPayload().toByteArray());
    } catch (IllegalArgumentException ignored) {
      return null;
    }
    return handleInit(connectionKey, inbound, decodedPayload);
  }

  private InboundPacket handleHandshake(ConnectionKey connectionKey, AuthenticatedConnectionState connectionState, InboundPacket inbound) {
    AuthenticatedHandshakeEnvelope decodedPayload;
    if (inbound.packet().getPacketType() != DpchPacketType.DPCH_PACKET_TYPE_DATA) {
      return null;
    }
    try {
      decodedPayload = AuthenticatedPayloadUtil.decodeEcdsa(inbound.packet().getPayload().toByteArray());
    } catch (IllegalArgumentException ignored) {
      return null;
    }

    if (decodedPayload == null) {
      return null;
    }

    return switch (connectionState.decideHandshake(decodedPayload.getAuthOpcode(), decodedPayload.getSenderId(), context.localSenderId)) {
      case USE_REPLY -> handleReply(connectionKey, inbound, decodedPayload);
      case RESTART -> handleInit(connectionKey, inbound, decodedPayload);
      case IGNORE -> null;
    };
  }

  private InboundPacket handleInit(ConnectionKey connectionKey, InboundPacket inbound, AuthenticatedHandshakeEnvelope decodedPayload) {
    if (decodedPayload == null || decodedPayload.getAuthOpcode() != AuthOpcode.AUTH_OPCODE_INIT) {
      return null;
    }

    PublicKey senderStaticPublicKey = context.getStaticPublicKey(decodedPayload.getSenderId());
    if (senderStaticPublicKey == null) {
      return null;
    }
    try {
      if (!AuthenticatedPayloadUtil.verifyEcdsa(decodedPayload, senderStaticPublicKey)) {
        return null;
      }
    } catch (Exception ignored) {
      return null;
    }

    PublicKey peerEphemeralPublicKey;
    try {
      peerEphemeralPublicKey = PublicKeyLoader.decodePublicKey(decodedPayload.getEphemeralPublicKeyBytes().toByteArray());
    } catch (Exception ignored) {
      return null;
    }

    KeyPair localEphemeralKeys;
    try {
      localEphemeralKeys = CryptoUtil.newECKeyPair();
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
    return null;
  }

  private InboundPacket handleReply(ConnectionKey connectionKey, InboundPacket inbound, AuthenticatedHandshakeEnvelope decodedPayload) {
    if (decodedPayload.getAuthOpcode() != AuthOpcode.AUTH_OPCODE_REPLY) {
      return null;
    }

    PublicKey responderStaticPublicKey = context.getStaticPublicKey(decodedPayload.getSenderId());
    if (responderStaticPublicKey == null) {
      return null;
    }
    try {
      if (!AuthenticatedPayloadUtil.verifyEcdsa(decodedPayload, responderStaticPublicKey)) {
        return null;
      }
    } catch (Exception ignored) {
      return null;
    }

    PublicKey peerEphemeralPublicKey;
    try {
      peerEphemeralPublicKey = PublicKeyLoader.decodePublicKey(decodedPayload.getEphemeralPublicKeyBytes().toByteArray());
    } catch (Exception ignored) {
      return null;
    }

    AuthenticatedConnectionState connectionState = context.getConnectionStateOrNull(connectionKey);
    if (connectionState == null) {
      return null;
    }

    PrivateKey localEphemeralPrivateKey = connectionState.ephemeralPrivateKey();
    if (localEphemeralPrivateKey == null) {
      return null;
    }

    SecretKey sharedSecret;
    try {
      CryptoUtil.KeyContext keyContext = new CryptoUtil.KeyContext("SESSION_" + connectionKey.connectionId(), "HANDSHAKE");
      sharedSecret = CryptoUtil.deriveCommonKey(localEphemeralPrivateKey, peerEphemeralPublicKey, keyContext);
    } catch (Exception exception) {
      throw new IllegalStateException("Failed to derive shared secret", exception);
    }
    List<byte[]> queuedPayloads = connectionState.finishHandshake(sharedSecret, decodedPayload.getSenderId());
    sender.sendQueuedPayloads(connectionKey, connectionState, queuedPayloads, inbound.sender());
    return null;
  }

  private InboundPacket handleData(AuthenticatedConnectionState connectionState, InboundPacket inbound) {
    if (inbound.packet().getPacketType() != DpchPacketType.DPCH_PACKET_TYPE_DATA) {
      return null;
    }

    AuthenticatedDataEnvelope decodedPayload;
    try {
      decodedPayload = AuthenticatedPayloadUtil.decodeHmac(inbound.packet().getPayload().toByteArray());
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

    DpchPacket decodedPacket = DpchPacket.newBuilder().setConnectionId(inbound.packet().getConnectionId()).setPacketType(DpchPacketType.DPCH_PACKET_TYPE_DATA).setHasAck(false)
        .setSequenceNumber(inbound.packet().getSequenceNumber()).setPayload(decodedPayload.getApplicationPayload()).build();
    return new InboundPacket(inbound.sender(), decodedPacket, connectionState.authenticatedRemoteSenderId());
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
