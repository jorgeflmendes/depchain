package pt.ulisboa.depchain.shared.network.links.authenticated;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.net.InetSocketAddress;
import java.security.KeyPair;

import pt.ulisboa.depchain.proto.AuthOpcode;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class AuthenticatedSender {
  private final AuthenticatedContext context;

  AuthenticatedSender(AuthenticatedContext context) {
    this.context = ValidationUtils.requireNonNull(context, "context");
  }

  void send(long connectionId, byte[] payload, InetSocketAddress remoteEndpoint) {
    ValidationUtils.requireAllNonNull(named("payload", payload), named("remoteEndpoint", remoteEndpoint));
    ConnectionKey connectionKey = new ConnectionKey(remoteEndpoint, connectionId);
    AuthenticatedConnectionState connectionState = context.getOrCreateConnectionState(connectionKey);
    AuthenticatedConnectionState.SendPlan sendPlan = connectionState.planSend(payload);

    switch (sendPlan.action()) {
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
      connectionState.beginClose();
    }
    try {
      context.handshakedLink.closeConnection(connectionId, remoteEndpoint);
    } finally {
      if (connectionState != null) {
        connectionState.close();
      }
    }
  }

  private void sendInit(ConnectionKey connectionKey, AuthenticatedConnectionState connectionState, InetSocketAddress remoteEndpoint) {
    ValidationUtils.requireAllNonNull(named("connectionKey", connectionKey), named("connectionState", connectionState), named("remoteEndpoint", remoteEndpoint));

    KeyPair localEKeys;
    try {
      localEKeys = CryptoUtil.newECKeyPair();
    } catch (Exception exception) {
      throw new IllegalStateException("Failed to allocate authenticated handshake key pair", exception);
    }

    if (!connectionState.tryMarkHandshakeInitiated(localEKeys.getPrivate())) {
      return;
    }

    try {
      byte[] initPayload = AuthenticatedPayloadUtil.encodeEcdsa(AuthOpcode.AUTH_OPCODE_INIT, context.localSenderId, localEKeys.getPublic(), context.localStaticSKey);
      context.handshakedLink.send(connectionKey.connectionId(), initPayload, remoteEndpoint);
    } catch (Exception exception) {
      connectionState.rollbackHandshake(localEKeys.getPrivate());
      throw new IllegalStateException("Failed to send authenticated init", exception);
    }
  }

  void sendReply(ConnectionKey connectionKey, KeyPair localEKeys, InetSocketAddress remoteEndpoint) {
    ValidationUtils.requireAllNonNull(named("connectionKey", connectionKey), named("localEKeys", localEKeys), named("remoteEndpoint", remoteEndpoint));

    try {
      byte[] replyPayload = AuthenticatedPayloadUtil.encodeEcdsa(AuthOpcode.AUTH_OPCODE_REPLY, context.localSenderId, localEKeys.getPublic(), context.localStaticSKey);
      context.handshakedLink.send(connectionKey.connectionId(), replyPayload, remoteEndpoint);
    } catch (Exception exception) {
      throw new IllegalStateException("Failed to send authenticated reply", exception);
    }
  }

  void sendQueuedPayloads(ConnectionKey connectionKey, AuthenticatedConnectionState connectionState,
      java.util.List<byte[]> queuedPayloads, InetSocketAddress remoteEndpoint) {
    ValidationUtils.requireAllNonNull(named("connectionKey", connectionKey), named("connectionState", connectionState),
        named("queuedPayloads", queuedPayloads), named("remoteEndpoint", remoteEndpoint));

    for (byte[] queuedPayload : queuedPayloads) {
      sendData(connectionKey, connectionState, queuedPayload, remoteEndpoint);
    }
  }

  private void sendData(ConnectionKey connectionKey, AuthenticatedConnectionState connectionState, byte[] payload, InetSocketAddress remoteEndpoint) {
    ValidationUtils.requireAllNonNull(named("connectionKey", connectionKey), named("payload", payload), named("remoteEndpoint", remoteEndpoint));

    AuthenticatedConnectionState.SecureSendContext secureSend = connectionState.reserveSecureSend();

    try {
      byte[] securePayload = AuthenticatedPayloadUtil.encodeHmac(AuthOpcode.AUTH_OPCODE_DATA, payload, secureSend.sharedSecret(), secureSend.nonce());
      context.handshakedLink.send(connectionKey.connectionId(), securePayload, remoteEndpoint);
    } catch (Exception exception) {
      throw new IllegalStateException("Failed to send authenticated data", exception);
    }
  }
}

