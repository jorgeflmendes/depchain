package pt.ulisboa.depchain.server.api;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;

import pt.ulisboa.depchain.proto.Message;
import pt.ulisboa.depchain.server.consensus.network.ReplicaTransportIds;
import pt.ulisboa.depchain.server.consensus.network.SerializedPeerSender;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.validation.ProtoValidationUtil;
import pt.ulisboa.depchain.shared.validation.ValidationUtils;

public final class ReplicaPeerApi {
  private final int localSenderId;
  private final Map<Integer, InetSocketAddress> consensusEndpointsBySenderId;
  private final Logger logger;
  private final SerializedPeerSender peerSendScheduler;

  private AuthenticatedLink nodeTransport;

  public ReplicaPeerApi(int localSenderId, ConfigParser config, Logger logger) {
    this.localSenderId = localSenderId;
    this.consensusEndpointsBySenderId = ReplicaTransportIds.buildConsensusEndpointsBySenderId(config);
    this.logger = ValidationUtils.requireNonNull(logger, "logger");
    this.peerSendScheduler = new SerializedPeerSender("replica-send-" + localSenderId, this.logger);
  }

  public void bindTransport(AuthenticatedLink nodeTransport) {
    this.nodeTransport = nodeTransport;
  }

  public void broadcastMessage(Message message) {
    if (nodeTransport == null) {
      return;
    }

    byte[] payload = ProtoValidationUtil.requireValid(message, "ReplicaMessage").toByteArray();
    for (Map.Entry<Integer, InetSocketAddress> entry : consensusEndpointsBySenderId.entrySet()) {
      if (entry.getKey() == localSenderId) {
        continue;
      }

      sendAsync(entry.getKey(), message.getViewNumber(), payload, entry.getValue(), "broadcast");
    }
  }

  public void sendMessageToReplica(int replicaSenderId, Message message) {
    if (nodeTransport == null) {
      return;
    }

    byte[] payload = ProtoValidationUtil.requireValid(message, "ReplicaMessage").toByteArray();
    sendAsync(replicaSenderId, message.getViewNumber(), payload, requireConsensusEndpoint(replicaSenderId), "send");
  }

  public int[] getCandidateReplicaSenderIds(int preferredSourceSenderId) {
    List<Integer> candidateSenderIds = new ArrayList<>();
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
    return ReplicaTransportIds.connectionIdForView(messageViewNumber, localSenderId, remoteSenderId, ReplicaTransportIds.REPLICA_MESSAGE_LANE);
  }

  private void sendAsync(int replicaSenderId, int messageViewNumber, byte[] payload, InetSocketAddress endpoint, String operation) {
    peerSendScheduler.schedule(replicaSenderId, "replica " + operation, () -> {
      if (nodeTransport == null) {
        return;
      }
      nodeTransport.send(replicaConnectionId(replicaSenderId, messageViewNumber), payload, endpoint);
    });
  }
}
