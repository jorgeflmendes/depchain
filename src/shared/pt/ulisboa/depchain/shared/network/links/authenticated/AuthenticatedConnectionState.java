package pt.ulisboa.depchain.shared.network.links.authenticated;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.crypto.SecretKey;

import pt.ulisboa.depchain.shared.network.links.RunOnce;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class AuthenticatedConnectionState {
  enum SendAction {
    SEND,
    START_HANDSHAKE,
    WAIT
  }

  enum ReceiveMode {
    DATA,
    HANDSHAKE,
    INIT
  }

  enum HandshakeAction {
    USE_REPLY,
    RESTART,
    IGNORE
  }

  record SendPlan(SendAction action) {
    SendPlan {
      ValidationUtils.requireNonNull(action, "action");
    }
  }

  record SecureSendContext(SecretKey sharedSecret, long nonce) {
    SecureSendContext {
      ValidationUtils.requireNonNull(sharedSecret, "sharedSecret");
      ValidationUtils.requireNonNegativeLong(nonce, "nonce");
    }
  }

  enum Phase {
    NEW,
    INITIATED,
    ESTABLISHED,
    CLOSING,
    CLOSED
  }

  private long sentNonce;
  private long receivedNonce;
  private final List<byte[]> pendingPayloads = new ArrayList<>();

  private Phase phase = Phase.NEW;
  private PrivateKey ephemeralPrivateKey;
  private SecretKey sharedSecret;
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

  synchronized SendPlan planSend(byte[] payload) {
    ValidationUtils.requireAllNonNull(named("payload", payload));
    if (phase == Phase.CLOSING || phase == Phase.CLOSED) {
      throw new IllegalStateException("Connection is closed");
    }

    if (phase == Phase.ESTABLISHED && sharedSecret != null) {
      return new SendPlan(SendAction.SEND);
    }

    pendingPayloads.add(Arrays.copyOf(payload, payload.length));
    if (phase == Phase.NEW) {
      return new SendPlan(SendAction.START_HANDSHAKE);
    }

    return new SendPlan(SendAction.WAIT);
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

  synchronized boolean canReceive() {
    return (phase == Phase.ESTABLISHED || phase == Phase.CLOSING) && sharedSecret != null;
  }

  synchronized boolean canSend() {
    return phase == Phase.ESTABLISHED;
  }

  synchronized boolean hasHandshakeInFlight() {
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

  synchronized List<byte[]> finishHandshake(SecretKey secretKey) {
    if (phase == Phase.CLOSED) {
      return List.of();
    }

    sharedSecret = ValidationUtils.requireNonNull(secretKey, "secretKey");
    ephemeralPrivateKey = null;
    phase = Phase.ESTABLISHED;
    return List.copyOf(takePendingPayloadsLocked());
  }

  synchronized SecretKey sharedSecret() {
    return sharedSecret;
  }

  synchronized HandshakeAction decideHandshake(AuthOpcode opcode, long remoteSenderId, long localSenderId) {
    ValidationUtils.requireNonNull(opcode, "opcode");
    ValidationUtils.requireNonNegativeLong(remoteSenderId, "remoteSenderId");
    ValidationUtils.requireNonNegativeLong(localSenderId, "localSenderId");
    if (phase != Phase.INITIATED || ephemeralPrivateKey == null) {
      return HandshakeAction.IGNORE;
    }

    if (opcode == AuthOpcode.REPLY) {
      return HandshakeAction.USE_REPLY;
    }

    if (opcode != AuthOpcode.INIT || remoteSenderId <= localSenderId) {
      return HandshakeAction.IGNORE;
    }

    resetHandshakeLocked();
    return HandshakeAction.RESTART;
  }

  synchronized SecureSendContext reserveSecureSend() {
    if (!isEstablished()) {
      throw new IllegalStateException("Connection is not established");
    }

    return new SecureSendContext(sharedSecret, ++sentNonce);
  }

  synchronized boolean validateAndIncrementReceivedNonce(long nonce) {
    if (!canReceive()) {
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
    pendingPayloads.clear();
    onTerminal.run();
  }

  synchronized List<byte[]> takePendingPayloads() {
    return new ArrayList<>(takePendingPayloadsLocked());
  }

  private List<byte[]> takePendingPayloadsLocked() {
    List<byte[]> queuedPayloads = new ArrayList<>(pendingPayloads);
    pendingPayloads.clear();
    return queuedPayloads;
  }

  private void resetHandshakeLocked() {
    if (phase == Phase.CLOSED) {
      return;
    }

    ephemeralPrivateKey = null;
    if (sharedSecret == null) {
      phase = Phase.NEW;
    }
  }
}

