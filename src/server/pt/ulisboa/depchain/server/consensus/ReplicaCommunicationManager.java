package pt.ulisboa.depchain.server.consensus;

import java.net.InetSocketAddress;
import java.util.Map;

import org.slf4j.Logger;

import pt.ulisboa.depchain.proto.Message;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.utils.ProtoValidationUtil;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class ReplicaCommunicationManager {
  private final int localSenderId;
  private final Map<Integer, InetSocketAddress> consensusEndpointsBySenderId;
  private final Logger logger;

  private AuthenticatedLink nodeTransport;

  ReplicaCommunicationManager(int localSenderId, ConfigParser config, Logger logger) {
    this.localSenderId = localSenderId;
    this.consensusEndpointsBySenderId = ConsensusTransportUtil.buildConsensusEndpointsBySenderId(config);
    this.logger = ValidationUtils.requireNonNull(logger, "logger");
  }

  void init(AuthenticatedLink nodeTransport) {
    this.nodeTransport = nodeTransport;
  }

  void broadcast(Message message) {
    if (nodeTransport == null) {
      return;
    }

    byte[] payload = ProtoValidationUtil.requireValid(message, "ReplicaMessage").toByteArray();
    for (Map.Entry<Integer, InetSocketAddress> entry : consensusEndpointsBySenderId.entrySet()) {
      if (entry.getKey() == localSenderId) {
        continue;
      }

      try {
        nodeTransport.send(replicaConnectionId(entry.getKey(), message.getViewNumber()), payload, entry.getValue());
      } catch (Exception exception) {
        logger.error("Error in broadcast to replica {}", entry.getKey(), exception);
      }
    }
  }

  void sendToReplica(int replicaSenderId, Message message) {
    if (nodeTransport == null) {
      return;
    }

    try {
      byte[] payload = ProtoValidationUtil.requireValid(message, "ReplicaMessage").toByteArray();
      nodeTransport.send(replicaConnectionId(replicaSenderId, message.getViewNumber()), payload, requireConsensusEndpoint(replicaSenderId));
    } catch (Exception exception) {
      logger.error("Error sending message to replica {}", replicaSenderId, exception);
    }
  }

  int[] candidateReplicaSenderIds(int preferredSourceSenderId) {
    java.util.List<Integer> candidateSenderIds = new java.util.ArrayList<>();
    if (preferredSourceSenderId >= 0 && preferredSourceSenderId != localSenderId && consensusEndpointsBySenderId.containsKey(preferredSourceSenderId)) {
      candidateSenderIds.add(preferredSourceSenderId);
    }
    for (Integer senderId : consensusEndpointsBySenderId.keySet()) {
      if (senderId != localSenderId && senderId != preferredSourceSenderId) {
        candidateSenderIds.add(senderId);
      }
    }
    return candidateSenderIds.stream().mapToInt(Integer::intValue).toArray();
  }

  private InetSocketAddress requireConsensusEndpoint(int senderId) {
    InetSocketAddress endpoint = consensusEndpointsBySenderId.get(senderId);
    if (endpoint == null) {
      throw new IllegalArgumentException("Unknown consensus endpoint for senderId " + senderId);
    }
    return endpoint;
  }

  private long replicaConnectionId(int remoteSenderId, int messageViewNumber) {
    return ConsensusTransportUtil.connectionIdForView(messageViewNumber, localSenderId, remoteSenderId, ConsensusTransportUtil.REPLICA_MESSAGE_LANE);
  }
}
