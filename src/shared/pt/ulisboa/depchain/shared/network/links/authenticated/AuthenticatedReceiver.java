package pt.ulisboa.depchain.shared.network.links.authenticated;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;

import javax.crypto.SecretKey;

import pt.ulisboa.depchain.shared.keys.PublicKeyLoader;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedPayload.DecodedEcdsaPayload;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedPayload.DecodedHmacPayload;
import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.dpch.DpchType;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class AuthenticatedReceiver {
  private final AuthenticatedContext context;
  private final AuthenticatedSender sender;

  AuthenticatedReceiver(AuthenticatedContext context, AuthenticatedSender sender) {
    ValidationUtils.requireAllNonNull(named("context", context), named("sender", sender));
    this.context = context;
    this.sender = sender;
  }

  InboundPacket receive() throws InterruptedException {
    return handleInbound(context.handshakedLink.receive());
  }

  InboundPacket receive(long timeoutMs) throws InterruptedException {
    return handleInbound(context.handshakedLink.receive(timeoutMs));
  }

  private InboundPacket handleInbound(InboundPacket inbound) {
    if (inbound == null) {
      return null;
    }

    ConnectionKey connectionKey = new ConnectionKey(inbound.sender(), inbound.packet().connectionId());
    if (context.hasSharedSecret(connectionKey)) { // Already Handshaked
      return handleSecureData(connectionKey, inbound);
    } else if (context.hasEphemeralPrivateKey(connectionKey)) { // Handshake initiated, waiting for reply
      return handleHandshakeReply(connectionKey, inbound);
    } else { // No handshake, expecting initiation
      return handleHandshakeInit(connectionKey, inbound);
    }
  }

  private InboundPacket handleHandshakeInit(ConnectionKey connectionKey, InboundPacket inbound) {
    if (inbound.packet().type() != DpchType.DATA) {
      return null;
    }

    // The first packet must contain an ECDSA-signed ephemeral key.
    DecodedEcdsaPayload decodedPayload = AuthenticatedPayload.decodeEcdsa(inbound.packet().payload());
    if (decodedPayload.opcode() != AuthOpcode.INIT) {
      return null;
    }

    // Get the sender's static public key using the sender ID from the decoded payload.
    long senderId = decodedPayload.senderId();
    PublicKey senderStaticPKey = context.getStaticPublicKey(senderId);
    if (senderStaticPKey == null) {
      return null;
    }

    // Only trust the peer ephemeral key if the static signature is valid.
    boolean isValidEcdsaHandshake;
    try {
      isValidEcdsaHandshake = AuthenticatedPayload.verifyEcdsa(inbound.packet().payload(), senderStaticPKey);
    } catch (Exception ignored) {
      isValidEcdsaHandshake = false;
    }
    if (!isValidEcdsaHandshake) {
      return null;
    }

    // Decode the sender's ephemeral public key.
    PublicKey peerEphemeralPKey;
    try {
      peerEphemeralPKey = PublicKeyLoader.decodePublicKey(decodedPayload.publicKeyBytes());
    } catch (Exception exception) {
      throw new IllegalArgumentException("Invalid encoded public key", exception);
    }

    // Generate our own ephemeral key pair for this handshake.
    KeyPair localEKeys;
    try {
      localEKeys = CryptoUtil.newECKeyPair();
    } catch (Exception exception) {
      throw new IllegalStateException("Failed to create ephemeral EC key pair", exception);
    }

    // Derive the shared secret (K) via ECDH using both ephemeral keys.
    SecretKey sharedSecret;
    try {
      CryptoUtil.KeyContext hkdfContext = newHandshakeKeyContext(connectionKey);
      sharedSecret = CryptoUtil.deriveCommonKey(localEKeys.getPrivate(), peerEphemeralPKey, hkdfContext);
    } catch (Exception exception) {
      throw new IllegalStateException("Failed to derive shared secret", exception);
    }

    // Send the handshake reply containing our ECDSA-signed ephemeral public key and save the shared
    // secret K.
    sender.sendHandshakeReply(connectionKey, localEKeys, inbound.sender());
    context.putSharedSecret(connectionKey, sharedSecret);
    context.ensureNonceState(connectionKey);
    return null;
  }

  private InboundPacket handleHandshakeReply(ConnectionKey connectionKey, InboundPacket inbound) {
    if (inbound.packet().type() != DpchType.DATA) {
      return null;
    }

    // The reply must contain an ECDSA-signed ephemeral key.
    DecodedEcdsaPayload decodedPayload = AuthenticatedPayload.decodeEcdsa(inbound.packet().payload());
    if (decodedPayload.opcode() != AuthOpcode.REPLY) {
      return null;
    }

    // Retrieve the responder's static public key using the sender ID from the decoded payload.
    long responderId = decodedPayload.senderId();
    PublicKey responderStaticPKey = context.getStaticPublicKey(responderId);
    if (responderStaticPKey == null) {
      return null;
    }

    // Verify the handshake reply signature using the responder's static public key.
    boolean isValidEcdsaReply;
    try {
      isValidEcdsaReply = AuthenticatedPayload.verifyEcdsa(inbound.packet().payload(), responderStaticPKey);
    } catch (Exception ignored) {
      isValidEcdsaReply = false;
    }
    if (!isValidEcdsaReply) {
      return null;
    }

    // Decode the responder's ephemeral public key.
    PublicKey peerEphemeralPKey;
    try {
      peerEphemeralPKey = PublicKeyLoader.decodePublicKey(decodedPayload.publicKeyBytes());
    } catch (Exception exception) {
      throw new IllegalArgumentException("Invalid encoded public key", exception);
    }

    // Retrieve our pending ephemeral private key generated during HANDSHAKE_INIT.
    PrivateKey localEphemeralSKey = context.getEphemeralPrivateKey(connectionKey);
    if (localEphemeralSKey == null) {
      return null;
    }

    // Derive the shared secret (K) via ECDH using both ephemeral keys.
    SecretKey sharedSecret;
    try {
      CryptoUtil.KeyContext hkdfContext = newHandshakeKeyContext(connectionKey);
      sharedSecret = CryptoUtil.deriveCommonKey(localEphemeralSKey, peerEphemeralPKey, hkdfContext);
    } catch (Exception exception) {
      throw new IllegalStateException("Failed to derive shared SecretKey K", exception);
    }

    // Save the shared secret (K) in the connection context for subsequent HMAC validations.
    context.putSharedSecret(connectionKey, sharedSecret);

    // Destroy the pending ephemeral private key from memory (perfect forward secrecy).
    context.removeEphemeralPrivateKey(connectionKey);
    context.ensureNonceState(connectionKey);
    sender.flushPendingPayloads(connectionKey, inbound.sender());

    return null;
  }

  private InboundPacket handleSecureData(ConnectionKey connectionKey, InboundPacket inbound) {
    if (inbound.packet().type() != DpchType.DATA) {
      return null;
    }

    // Secure data must contain an HMAC-protected payload.
    DecodedHmacPayload decodedPayload = AuthenticatedPayload.decodeHmac(inbound.packet().payload());
    if (decodedPayload.opcode() != AuthOpcode.DATA) {
      return null;
    }

    // Verify the HMAC using the shared secret K for this connection.
    boolean isValidHmac;
    try {
      isValidHmac = AuthenticatedPayload.verifyHmac(inbound.packet().payload(), context.getSharedSecret(connectionKey));
    } catch (Exception ignored) {
      isValidHmac = false;
    }
    if (!isValidHmac) {
      return null;
    }

    // Validate the nonce to protect against replay attacks.
    long expectedNonce = context.getReceivedNonce(connectionKey) + 1;
    if (decodedPayload.nonce() != expectedNonce) {
      return null;
    }

    context.incrementReceivedNonce(connectionKey);
    Dpch decodedPacket = Dpch.from(inbound.packet().connectionId(), inbound.packet().type(), inbound.packet().sequenceNumber(), decodedPayload.payload());
    return new InboundPacket(inbound.sender(), decodedPacket);
  }

  private static CryptoUtil.KeyContext newHandshakeKeyContext(ConnectionKey connectionKey) {
    ValidationUtils.requireNonNull(connectionKey, "connectionKey");
    return new CryptoUtil.KeyContext("SESSION_" + connectionKey.connectionId(), "HANDSHAKE");
  }
}
