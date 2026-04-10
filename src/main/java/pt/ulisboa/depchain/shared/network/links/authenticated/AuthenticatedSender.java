package pt.ulisboa.depchain.shared.network.links.authenticated;

import static pt.ulisboa.depchain.shared.validation.ValidationUtils.named;

import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.util.List;

import pt.ulisboa.depchain.proto.AuthOpcode;
import pt.ulisboa.depchain.shared.crypto.CryptoUtil;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.validation.ValidationUtils;

final class AuthenticatedSender {
  private final AuthenticatedContext context;

  AuthenticatedSender(AuthenticatedContext context) {
    this.context = ValidationUtils.requireNonNull(context, "context");
  }

  void send(long connectionId, byte[] payload, InetSocketAddress remoteEndpoint) {
    ValidationUtils.requireAllNonNull(named("payload", payload), named("remoteEndpoint", remoteEndpoint));
    ConnectionKey connectionKey = new ConnectionKey(remoteEndpoint, connectionId);
    AuthenticatedConnectionState connectionState = context.getOrCreateConnectionState(connectionKey);
    AuthenticatedConnectionState.SendAction sendAction = connectionState.planSend(payload);

    switch (sendAction) {
      case SEND -> sendData(connectionKey, connectionState, payload, remoteEndpoint);
      case START_HANDSHAKE -> sendInit(connectionKey, connectionState, remoteEndpoint);
      case WAIT -> {
      }
    }
  }

  void closeConnection(long connectionId, InetSocketAddress remoteEndpoint) {
    ValidationUtils.requireNonNull(remoteEndpoint, "remoteEndpoint");
    ConnectionKey connectionKey = new ConnectionKey(remoteEndpoint, connectionId);
    AuthenticatedConnectionState connectionState = context.getConnectionStateOrNull(connectionKey);
    if (connectionState != null) {
      if (connectionState.isAwaitingReply()) {
        context.signalAsyncHandshakeCompleted();
      }
      connectionState.beginClose();
    }
    try {
      context.perfectLink.cancelPendingData(connectionId, remoteEndpoint);
      context.perfectLink.releaseConnection(connectionId, remoteEndpoint);
    } finally {
      if (connectionState != null) {
        connectionState.close();
      }
    }
  }

  void ensureHandshake(ConnectionKey connectionKey, InetSocketAddress remoteEndpoint) {
    ValidationUtils.requireAllNonNull(named("connectionKey", connectionKey), named("remoteEndpoint", remoteEndpoint));
    AuthenticatedConnectionState connectionState = context.getOrCreateConnectionState(connectionKey);
    if (connectionState.isEstablished() || connectionState.isAwaitingReply()) {
      return;
    }
    sendInit(connectionKey, connectionState, remoteEndpoint);
  }

  private void sendInit(ConnectionKey connectionKey, AuthenticatedConnectionState connectionState, InetSocketAddress remoteEndpoint) {
    ValidationUtils.requireAllNonNull(named("connectionKey", connectionKey), named("connectionState", connectionState), named("remoteEndpoint", remoteEndpoint));

    KeyPair localEKeys;
    try {
      localEKeys = CryptoUtil.createEcKeyPair();
    } catch (Exception exception) {
      throw new IllegalStateException("Failed to allocate authenticated handshake key pair", exception);
    }

    if (!connectionState.tryMarkHandshakeInitiated(localEKeys.getPrivate())) {
      return;
    }
    context.signalAsyncHandshakeStarted();

    try {
      byte[] initPayload = AuthenticatedPayloadUtil.encodeEcdsa(AuthOpcode.AUTH_OPCODE_INIT, context.localSenderId, localEKeys.getPublic(), context.localStaticSKey);
      context.perfectLink.send(connectionKey.connectionId(), initPayload, remoteEndpoint);
    } catch (Exception exception) {
      context.signalAsyncHandshakeCompleted();
      connectionState.rollbackHandshake(localEKeys.getPrivate());
      connectionState.close();
      throw new IllegalStateException("Failed to send authenticated init", exception);
    }
  }

  void sendReply(ConnectionKey connectionKey, KeyPair localEKeys, InetSocketAddress remoteEndpoint) {
    ValidationUtils.requireAllNonNull(named("connectionKey", connectionKey), named("localEKeys", localEKeys), named("remoteEndpoint", remoteEndpoint));

    try {
      byte[] replyPayload = AuthenticatedPayloadUtil.encodeEcdsa(AuthOpcode.AUTH_OPCODE_REPLY, context.localSenderId, localEKeys.getPublic(), context.localStaticSKey);
      context.perfectLink.send(connectionKey.connectionId(), replyPayload, remoteEndpoint);
    } catch (Exception exception) {
      AuthenticatedConnectionState state = context.getConnectionStateOrNull(connectionKey);
      if (state != null) {
        state.close();
      }
      throw new IllegalStateException("Failed to send authenticated reply", exception);
    }
  }

  void sendQueuedPayloads(ConnectionKey connectionKey, AuthenticatedConnectionState connectionState, List<byte[]> queuedPayloads, InetSocketAddress remoteEndpoint) {
    ValidationUtils
        .requireAllNonNull(named("connectionKey", connectionKey), named("connectionState", connectionState), named("queuedPayloads", queuedPayloads), named("remoteEndpoint", remoteEndpoint));

    for (byte[] queuedPayload : queuedPayloads) {
      sendData(connectionKey, connectionState, queuedPayload, remoteEndpoint);
    }
  }

  private void sendData(ConnectionKey connectionKey, AuthenticatedConnectionState connectionState, byte[] payload, InetSocketAddress remoteEndpoint) {
    ValidationUtils.requireAllNonNull(named("connectionKey", connectionKey), named("payload", payload), named("remoteEndpoint", remoteEndpoint));

    AuthenticatedConnectionState.SecureSendContext secureSend = connectionState.reserveSecureSend();

    try {
      byte[] securePayload = AuthenticatedPayloadUtil.encodeHmac(AuthOpcode.AUTH_OPCODE_DATA, payload, secureSend.sharedSecret(), secureSend.nonce());
      context.perfectLink.send(connectionKey.connectionId(), securePayload, remoteEndpoint);
    } catch (Exception exception) {
      connectionState.close();
      throw new IllegalStateException("Failed to send authenticated data", exception);
    }
  }
}
