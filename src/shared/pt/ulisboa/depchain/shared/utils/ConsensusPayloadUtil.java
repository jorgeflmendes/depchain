package pt.ulisboa.depchain.shared.utils;

import pt.ulisboa.depchain.proto.ConsensusMessageType;
import pt.ulisboa.depchain.proto.Node;
import pt.ulisboa.depchain.proto.NodeCommand;
import pt.ulisboa.depchain.proto.NodeHashPayload;
import pt.ulisboa.depchain.proto.ThresholdVotePayload;

public final class ConsensusPayloadUtil {
  private ConsensusPayloadUtil() {
  }

  public static byte[] votePayload(ConsensusMessageType type, int viewNumber, Node node) {
    ValidationUtils.requireNonNull(type, "type");
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");
    ValidationUtils.requireNonNull(node, "node");

    ThresholdVotePayload payload = ThresholdVotePayload.newBuilder().setMessageType(type).setViewNumber(viewNumber).setVotedNode(node).build();
    return ProtoValidationUtil.requireValid(payload, "ThresholdVotePayload").toByteArray();
  }

  public static byte[] nodeHashPayload(String parentHash, int viewNumber, NodeCommand command) {
    ValidationUtils.requireAllNonNull(ValidationUtils.named("parentHash", parentHash), ValidationUtils.named("command", command));
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");

    NodeHashPayload payload = NodeHashPayload.newBuilder().setParentNodeHash(parentHash).setViewNumber(viewNumber).setCommand(command).build();
    return ProtoValidationUtil.requireValid(payload, "NodeHashPayload").toByteArray();
  }
}
