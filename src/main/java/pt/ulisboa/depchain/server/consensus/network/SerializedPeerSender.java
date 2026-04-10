package pt.ulisboa.depchain.server.consensus.network;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import pt.ulisboa.depchain.shared.validation.ValidationUtils;

public final class SerializedPeerSender {
  private final String threadNamePrefix;
  private final Logger logger;
  private final Map<Integer, Object> sendLocksByReplicaSenderId;

  public SerializedPeerSender(String threadNamePrefix, Logger logger) {
    this.threadNamePrefix = ValidationUtils.requireNonBlank(threadNamePrefix, "threadNamePrefix");
    this.logger = ValidationUtils.requireNonNull(logger, "logger");
    this.sendLocksByReplicaSenderId = new ConcurrentHashMap<>();
  }

  public void schedule(int replicaSenderId, String operationDescription, PeerSendOperation sendOperation) {
    ValidationUtils.requireNonNegativeInt(replicaSenderId, "replicaSenderId");
    ValidationUtils.requireNonBlank(operationDescription, "operationDescription");
    ValidationUtils.requireNonNull(sendOperation, "sendOperation");

    Object sendLock = sendLocksByReplicaSenderId.computeIfAbsent(replicaSenderId, ignored -> new Object());
    Thread.ofVirtual().name(threadNamePrefix + "-" + replicaSenderId).start(() -> {
      synchronized (sendLock) {
        try {
          sendOperation.run();
        } catch (Exception exception) {
          logger.debug("Ignoring {} failure to replica {}", operationDescription, replicaSenderId, exception);
        }
      }
    });
  }

  @FunctionalInterface
  public interface PeerSendOperation {
    void run() throws Exception;
  }
}
