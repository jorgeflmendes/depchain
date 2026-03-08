package pt.ulisboa.depchain.shared.network.links.authenticated;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.util.Queue;

import pt.ulisboa.depchain.shared.utils.CryptoUtil;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class AuthenticatedSender {
  private final AuthenticatedContext context;

  AuthenticatedSender(AuthenticatedContext context) {
    this.context = ValidationUtils.requireNonNull(context, "context");
  }

  void send(long connectionId, byte[] payload, InetSocketAddress remoteEndpoint) {
    ValidationUtils.requireAllNonNull(named("payload", payload), named("remoteEndpoint", remoteEndpoint));
    // Use the same key shape as the lower layers to avoid peer collisions.
    ConnectionKey connectionKey = new ConnectionKey(remoteEndpoint, connectionId);
    if (context.hasSharedSecret(connectionKey)) { // Already handshaked, send secure data.
      sendSecureData(connectionKey, payload, remoteEndpoint);
    } else if (context.hasEphemeralPrivateKey(connectionKey)) { // Handshake initiated, queue payloads until reply arrives.
      context.enqueuePendingPayload(connectionKey, payload);
    } else { // No handshake, initiate it.
      context.enqueuePendingPayload(connectionKey, payload);
      sendHandshakeInit(connectionKey, remoteEndpoint);
    }
  }

  void closeConnection(long connectionId, InetSocketAddress remoteEndpoint) {
    ValidationUtils.requireNonNull(remoteEndpoint, "remoteEndpoint");
    context.handshakedLink.closeConnection(connectionId, remoteEndpoint);
  }

  private void sendHandshakeInit(ConnectionKey connectionKey, InetSocketAddress remoteEndpoint) {
    ValidationUtils.requireAllNonNull(named("connectionKey", connectionKey), named("remoteEndpoint", remoteEndpoint));

    try {
      KeyPair localEKeys = CryptoUtil.newECKeyPair();
      context.putEphemeralPrivateKey(connectionKey, localEKeys.getPrivate());
      byte[] initPayload = AuthenticatedPayload.encodeEcdsa(AuthOpcode.INIT, context.localSenderId, localEKeys.getPublic(), context.localStaticSKey);
      context.handshakedLink.send(connectionKey.connectionId(), initPayload, remoteEndpoint);
    } catch (Exception exception) {
      throw new IllegalStateException("Failed to send authenticated handshake init", exception);
    }
  }

  void sendHandshakeReply(ConnectionKey connectionKey, KeyPair localEKeys, InetSocketAddress remoteEndpoint) {
    ValidationUtils.requireAllNonNull(named("connectionKey", connectionKey), named("localEKeys", localEKeys), named("remoteEndpoint", remoteEndpoint));

    try {
      byte[] replyPayload = AuthenticatedPayload.encodeEcdsa(AuthOpcode.REPLY, context.localSenderId, localEKeys.getPublic(), context.localStaticSKey);
      context.handshakedLink.send(connectionKey.connectionId(), replyPayload, remoteEndpoint);
    } catch (Exception exception) {
      throw new IllegalStateException("Failed to send authenticated handshake reply", exception);
    }
  }

  void flushPendingPayloads(ConnectionKey connectionKey, InetSocketAddress remoteEndpoint) {
    ValidationUtils.requireAllNonNull(named("connectionKey", connectionKey), named("remoteEndpoint", remoteEndpoint));

    Queue<byte[]> pendingPayloads = context.removePendingPayloads(connectionKey);
    byte[] pendingPayload;
    while ((pendingPayload = pendingPayloads.poll()) != null) {
      sendSecureData(connectionKey, pendingPayload, remoteEndpoint);
    }
  }

  private void sendSecureData(ConnectionKey connectionKey, byte[] payload, InetSocketAddress remoteEndpoint) {
    ValidationUtils.requireAllNonNull(named("connectionKey", connectionKey), named("payload", payload), named("remoteEndpoint", remoteEndpoint));

    long nonce = context.incrementSentNonce(connectionKey);

    try {
      byte[] securePayload = AuthenticatedPayload.encodeHmac(AuthOpcode.DATA, payload, context.getSharedSecret(connectionKey), nonce);
      context.handshakedLink.send(connectionKey.connectionId(), securePayload, remoteEndpoint);
    } catch (Exception exception) {
      throw new IllegalStateException("Failed to send authenticated secure data", exception);
    }
  }
}
