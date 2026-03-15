package pt.ulisboa.depchain.shared.network.links.authenticated;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;

import javax.crypto.SecretKey;

import pt.ulisboa.depchain.proto.AuthOpcode;
import pt.ulisboa.depchain.proto.AuthenticatedDataEnvelope;
import pt.ulisboa.depchain.proto.AuthenticatedHandshakeEnvelope;
import pt.ulisboa.depchain.proto.DpchPacket;
import pt.ulisboa.depchain.proto.DpchPacketType;
import pt.ulisboa.depchain.shared.keys.PublicKeyLoader;
import pt.ulisboa.depchain.shared.logging.Logger;
import pt.ulisboa.depchain.shared.network.links.LinkClosedException;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class AuthenticatedReceiver {
  private static final Logger logger = new Logger("AuthenticatedReceiver");
  private final AuthenticatedContext context;
  private final AuthenticatedSender sender;

  AuthenticatedReceiver(AuthenticatedContext context, AuthenticatedSender sender) {
    ValidationUtils.requireAllNonNull(named("context", context), named("sender", sender));
    this.context = context;
    this.sender = sender;
  }

  void runInboundLoop() {
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
        logger.debug("AuthenticatedLink worker error: " + exception.getMessage());
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
    return handleInit(connectionKey, inbound);
  }

  // If both sides sent INIT, the higher senderId keeps the initiator role.
  private InboundPacket handleHandshake(ConnectionKey connectionKey, AuthenticatedConnectionState connectionState, InboundPacket inbound) {
    AuthenticatedHandshakeEnvelope decodedPayload = decodeHandshakePayload(inbound);
    if (decodedPayload == null) {
      return null;
    }

    return switch (connectionState.decideHandshake(decodedPayload.getAuthOpcode(), decodedPayload.getSenderId(), context.localSenderId)) {
      case USE_REPLY -> handleReply(connectionKey, inbound);
      case RESTART -> handleInit(connectionKey, inbound);
      case IGNORE -> null;
    };
  }

  private InboundPacket handleInit(ConnectionKey connectionKey, InboundPacket inbound) {
    return handleInit(connectionKey, inbound, decodeHandshakePayload(inbound));
  }

  private InboundPacket handleInit(ConnectionKey connectionKey, InboundPacket inbound, AuthenticatedHandshakeEnvelope decodedPayload) {
    if (decodedPayload == null || decodedPayload.getAuthOpcode() != AuthOpcode.AUTH_OPCODE_INIT) {
      return null;
    }

    // Get the sender's static public key using the sender ID from the decoded payload.
    long senderId = decodedPayload.getSenderId();
    PublicKey senderStaticPKey = context.getStaticPublicKey(senderId);
    if (senderStaticPKey == null) {
      return null;
    }

    // Only trust the peer ephemeral key if the static signature is valid.
    if (!verifyHandshakePayload(decodedPayload, senderStaticPKey)) {
      return null;
    }

    // Decode the sender's ephemeral public key.
    PublicKey peerEphemeralPKey = decodePublicKey(decodedPayload.getEphemeralPublicKeyBytes().toByteArray());
    if (peerEphemeralPKey == null) {
      return null;
    }

    // Generate our own ephemeral key pair for this handshake.
    KeyPair localEKeys;
    try {
      localEKeys = CryptoUtil.newECKeyPair();
    } catch (Exception exception) {
      throw new IllegalStateException("Failed to create ephemeral EC key pair", exception);
    }

    // Derive the shared secret (K) via ECDH using both ephemeral keys.
    SecretKey sharedSecret = deriveSharedSecret(connectionKey, localEKeys.getPrivate(), peerEphemeralPKey);

    sender.sendReply(connectionKey, localEKeys, inbound.sender());
    AuthenticatedConnectionState connectionState = context.getOrCreateConnectionState(connectionKey);
    java.util.List<byte[]> queuedPayloads = connectionState.finishHandshake(sharedSecret);
    sender.sendQueuedPayloads(connectionKey, connectionState, queuedPayloads, inbound.sender());
    return null;
  }

  private InboundPacket handleReply(ConnectionKey connectionKey, InboundPacket inbound) {
    AuthenticatedHandshakeEnvelope decodedPayload = decodeHandshakePayload(inbound);
    if (decodedPayload == null || decodedPayload.getAuthOpcode() != AuthOpcode.AUTH_OPCODE_REPLY) {
      return null;
    }

    // Retrieve the responder's static public key using the sender ID from the decoded payload.
    long responderId = decodedPayload.getSenderId();
    PublicKey responderStaticPKey = context.getStaticPublicKey(responderId);
    if (responderStaticPKey == null) {
      return null;
    }

    // Verify the handshake reply signature using the responder's static public key.
    if (!verifyHandshakePayload(decodedPayload, responderStaticPKey)) {
      return null;
    }

    // Decode the responder's ephemeral public key.
    PublicKey peerEphemeralPKey = decodePublicKey(decodedPayload.getEphemeralPublicKeyBytes().toByteArray());
    if (peerEphemeralPKey == null) {
      return null;
    }

    // Retrieve our pending ephemeral private key generated during HANDSHAKE_INIT.
    AuthenticatedConnectionState connectionState = context.getConnectionStateOrNull(connectionKey);
    if (connectionState == null) {
      return null;
    }

    PrivateKey localEphemeralSKey = connectionState.ephemeralPrivateKey();
    if (localEphemeralSKey == null) {
      return null;
    }

    // Derive the shared secret (K) via ECDH using both ephemeral keys.
    SecretKey sharedSecret = deriveSharedSecret(connectionKey, localEphemeralSKey, peerEphemeralPKey);

    java.util.List<byte[]> queuedPayloads = connectionState.finishHandshake(sharedSecret);
    sender.sendQueuedPayloads(connectionKey, connectionState, queuedPayloads, inbound.sender());

    return null;
  }

  private InboundPacket handleData(AuthenticatedConnectionState connectionState, InboundPacket inbound) {
    if (inbound.packet().getPacketType() != DpchPacketType.DPCH_PACKET_TYPE_DATA) {
      return null;
    }

    // Secure data must contain an HMAC-protected payload.
    AuthenticatedDataEnvelope decodedPayload;
    try {
      decodedPayload = AuthenticatedPayloadUtil.decodeHmac(inbound.packet().getPayload().toByteArray());
    } catch (IllegalArgumentException ignored) {
      return null;
    }

    if (decodedPayload.getAuthOpcode() != AuthOpcode.AUTH_OPCODE_DATA) {
      return null;
    }

    // Verify the HMAC using the shared secret K for this connection.
    boolean isValidHmac;
    try {
      isValidHmac = AuthenticatedPayloadUtil.verifyHmac(decodedPayload, connectionState.sharedSecret());
    } catch (Exception ignored) {
      isValidHmac = false;
    }
    if (!isValidHmac) {
      return null;
    }

    // Validate the nonce to protect against replay attacks.
    if (!connectionState.validateAndIncrementReceivedNonce(decodedPayload.getNonce())) {
      return null;
    }
    DpchPacket decodedPacket = DpchPacket.newBuilder().setConnectionId(inbound.packet().getConnectionId()).setPacketType(DpchPacketType.DPCH_PACKET_TYPE_DATA).setHasAck(false)
        .setSequenceNumber(inbound.packet().getSequenceNumber()).setPayload(decodedPayload.getApplicationPayload()).build();
    return new InboundPacket(inbound.sender(), decodedPacket);
  }

  private AuthenticatedHandshakeEnvelope decodeHandshakePayload(InboundPacket inbound) {
    if (inbound.packet().getPacketType() != DpchPacketType.DPCH_PACKET_TYPE_DATA) {
      return null;
    }

    try {
      return AuthenticatedPayloadUtil.decodeEcdsa(inbound.packet().getPayload().toByteArray());
    } catch (IllegalArgumentException ignored) {
      return null;
    }
  }

  private boolean verifyHandshakePayload(AuthenticatedHandshakeEnvelope payload, PublicKey staticPublicKey) {
    try {
      return AuthenticatedPayloadUtil.verifyEcdsa(payload, staticPublicKey);
    } catch (Exception ignored) {
      return false;
    }
  }

  private PublicKey decodePublicKey(byte[] publicKeyBytes) {
    try {
      return PublicKeyLoader.decodePublicKey(publicKeyBytes);
    } catch (Exception ignored) {
      return null;
    }
  }

  private SecretKey deriveSharedSecret(ConnectionKey connectionKey, PrivateKey localPrivateKey, PublicKey peerPublicKey) {
    try {
      CryptoUtil.KeyContext hkdfContext = newHandshakeKeyContext(connectionKey);
      return CryptoUtil.deriveCommonKey(localPrivateKey, peerPublicKey, hkdfContext);
    } catch (Exception exception) {
      throw new IllegalStateException("Failed to derive shared secret", exception);
    }
  }

  private static CryptoUtil.KeyContext newHandshakeKeyContext(ConnectionKey connectionKey) {
    ValidationUtils.requireNonNull(connectionKey, "connectionKey");
    return new CryptoUtil.KeyContext("SESSION_" + connectionKey.connectionId(), "HANDSHAKE");
  }
}

