package pt.ulisboa.depchain.shared.network.links.authenticated;

import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.crypto.SecretKey;

import pt.ulisboa.depchain.proto.AuthOpcode;
import pt.ulisboa.depchain.shared.network.links.RunOnce;
import pt.ulisboa.depchain.shared.validation.ValidationUtils;

final class AuthenticatedConnectionState {
  enum SendAction {
    SEND, START_HANDSHAKE, WAIT
  }

  enum ReceiveMode {
    DATA, HANDSHAKE, INIT
  }

  enum HandshakeAction {
    USE_REPLY, RESTART, IGNORE
  }

  record SecureSendContext(SecretKey sharedSecret, long nonce) {
    SecureSendContext {
      ValidationUtils.requireNonNull(sharedSecret, "sharedSecret");
      ValidationUtils.requireNonNegativeLong(nonce, "nonce");
    }
  }

  enum Phase {
    NEW, INITIATED, ESTABLISHED, CLOSING, CLOSED
  }

  private long sentNonce;
  private long receivedNonce;
  private final List<byte[]> pendingPayloads = new ArrayList<>();

  private Phase phase = Phase.NEW;
  private PrivateKey ephemeralPrivateKey;
  private SecretKey sharedSecret;
  private Long authenticatedRemoteSenderId;
  private final RunOnce onTerminal;

  AuthenticatedConnectionState() {
    this(() -> {
    });
  }

  AuthenticatedConnectionState(Runnable onTerminal) {
    ValidationUtils.requireNonNull(onTerminal, "onTerminal");
    this.onTerminal = new RunOnce(onTerminal);
  }

  synchronized boolean isEstablished() {
    return phase == Phase.ESTABLISHED && sharedSecret != null;
  }

  synchronized SendAction planSend(byte[] payload) {
    ValidationUtils.requireNonNull(payload, "payload");
    if (phase == Phase.CLOSING || phase == Phase.CLOSED) {
      throw new IllegalStateException("Connection is closed");
    }

    if (phase == Phase.ESTABLISHED && sharedSecret != null) {
      return SendAction.SEND;
    }

    pendingPayloads.add(Arrays.copyOf(payload, payload.length));
    if (phase == Phase.NEW) {
      return SendAction.START_HANDSHAKE;
    }

    return SendAction.WAIT;
  }

  synchronized ReceiveMode receiveMode() {
    if ((phase == Phase.ESTABLISHED || phase == Phase.CLOSING) && sharedSecret != null) {
      return ReceiveMode.DATA;
    }

    if (phase == Phase.INITIATED && ephemeralPrivateKey != null) {
      return ReceiveMode.HANDSHAKE;
    }

    return ReceiveMode.INIT;
  }

  synchronized boolean isAwaitingReply() {
    return phase == Phase.INITIATED && ephemeralPrivateKey != null;
  }

  synchronized boolean tryMarkHandshakeInitiated(PrivateKey privateKey) {
    if (phase != Phase.NEW) {
      return false;
    }

    ephemeralPrivateKey = ValidationUtils.requireNonNull(privateKey, "privateKey");
    phase = Phase.INITIATED;
    return true;
  }

  synchronized void rollbackHandshake(PrivateKey privateKey) {
    ValidationUtils.requireNonNull(privateKey, "privateKey");
    if (phase == Phase.INITIATED && ephemeralPrivateKey == privateKey) {
      ephemeralPrivateKey = null;
      if (sharedSecret == null) {
        phase = Phase.NEW;
      }
    }
  }

  synchronized PrivateKey ephemeralPrivateKey() {
    return ephemeralPrivateKey;
  }

  synchronized List<byte[]> finishHandshake(SecretKey secretKey, long remoteSenderId) {
    if (phase == Phase.CLOSED) {
      return List.of();
    }

    sharedSecret = ValidationUtils.requireNonNull(secretKey, "secretKey");
    authenticatedRemoteSenderId = remoteSenderId;
    ephemeralPrivateKey = null;
    phase = Phase.ESTABLISHED;
    return List.copyOf(drainPendingPayloads());
  }

  synchronized SecretKey sharedSecret() {
    return sharedSecret;
  }

  synchronized Long authenticatedRemoteSenderId() {
    return authenticatedRemoteSenderId;
  }

  synchronized HandshakeAction decideHandshake(AuthOpcode opcode, long remoteSenderId, long localSenderId) {
    ValidationUtils.requireNonNull(opcode, "opcode");
    ValidationUtils.requireNonNegativeLong(remoteSenderId, "remoteSenderId");
    ValidationUtils.requireNonNegativeLong(localSenderId, "localSenderId");
    if (phase == Phase.ESTABLISHED || phase == Phase.CLOSING) {
      if (opcode != AuthOpcode.AUTH_OPCODE_INIT) {
        return HandshakeAction.IGNORE;
      }
      if (authenticatedRemoteSenderId != null && authenticatedRemoteSenderId != remoteSenderId) {
        return HandshakeAction.IGNORE;
      }
      restartSessionForHandshake();
      return HandshakeAction.RESTART;
    }

    if (phase != Phase.INITIATED || ephemeralPrivateKey == null) {
      return HandshakeAction.IGNORE;
    }

    if (opcode == AuthOpcode.AUTH_OPCODE_REPLY) {
      return HandshakeAction.USE_REPLY;
    }

    if (opcode != AuthOpcode.AUTH_OPCODE_INIT || remoteSenderId <= localSenderId) {
      return HandshakeAction.IGNORE;
    }

    resetHandshakeState();
    return HandshakeAction.RESTART;
  }

  synchronized SecureSendContext reserveSecureSend() {
    if (!isEstablished()) {
      throw new IllegalStateException("Connection is not established");
    }

    return new SecureSendContext(sharedSecret, ++sentNonce);
  }

  synchronized boolean validateAndIncrementReceivedNonce(long nonce) {
    if ((phase != Phase.ESTABLISHED && phase != Phase.CLOSING) || sharedSecret == null) {
      return false;
    }

    long expectedNonce = receivedNonce + 1;
    if (nonce != expectedNonce) {
      return false;
    }

    receivedNonce = expectedNonce;
    return true;
  }

  synchronized void beginClose() {
    if (phase == Phase.CLOSED) {
      return;
    }

    phase = Phase.CLOSING;
    ephemeralPrivateKey = null;
    pendingPayloads.clear();
  }

  synchronized void close() {
    if (phase == Phase.CLOSED) {
      return;
    }

    phase = Phase.CLOSED;
    ephemeralPrivateKey = null;
    sharedSecret = null;
    authenticatedRemoteSenderId = null;
    pendingPayloads.clear();
    onTerminal.run();
  }

  private List<byte[]> drainPendingPayloads() {
    List<byte[]> queuedPayloads = new ArrayList<>(pendingPayloads);
    pendingPayloads.clear();
    return queuedPayloads;
  }

  private void resetHandshakeState() {
    if (phase == Phase.CLOSED) {
      return;
    }

    ephemeralPrivateKey = null;
    if (sharedSecret == null) {
      authenticatedRemoteSenderId = null;
      phase = Phase.NEW;
    }
  }

  private void restartSessionForHandshake() {
    if (phase == Phase.CLOSED) {
      return;
    }

    phase = Phase.NEW;
    ephemeralPrivateKey = null;
    sharedSecret = null;
    authenticatedRemoteSenderId = null;
    sentNonce = 0L;
    receivedNonce = 0L;
  }
}
