package pt.ulisboa.depchain.server.consensus.network;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class ReplicaTransportIds {
  public static final int REPLICA_MESSAGE_LANE = 1;
  public static final int THRESHOLD_MESSAGE_LANE = 2;

  private ReplicaTransportIds() {
  }

  public static Map<Integer, InetSocketAddress> buildConsensusEndpointsBySenderId(ConfigParser config) {
    ValidationUtils.requireNonNull(config, "config");

    Map<Integer, InetSocketAddress> endpointsBySenderId = new HashMap<>();
    for (ConfigParser.ReplicaSection replica : config.replicas()) {
      try {
        endpointsBySenderId.put(Math.toIntExact(replica.senderId()), new InetSocketAddress(InetAddress.getByName(replica.host()), replica.consensusPort()));
      } catch (UnknownHostException exception) {
        throw new IllegalStateException("Unable to resolve consensus host for replica " + replica.id(), exception);
      }
    }
    return Map.copyOf(endpointsBySenderId);
  }

  public static long connectionIdForView(int viewNumber, int localSenderId, int remoteSenderId, int lane) {
    ValidationUtils.requireAtLeastInt(viewNumber, -1, "viewNumber", "-1");
    ValidationUtils.requireNonNegativeInt(localSenderId, "localSenderId");
    ValidationUtils.requireNonNegativeInt(remoteSenderId, "remoteSenderId");
    ValidationUtils.requirePositiveInt(lane, "lane");

    long normalizedView = Integer.toUnsignedLong(viewNumber + 1);
    long normalizedLane = lane & 0xFFL;
    long normalizedLocalSenderId = localSenderId & 0xFFFL;
    long normalizedRemoteSenderId = remoteSenderId & 0xFFFL;
    return (normalizedView << 32) | (normalizedLane << 24) | (normalizedLocalSenderId << 12) | normalizedRemoteSenderId;
  }
}
