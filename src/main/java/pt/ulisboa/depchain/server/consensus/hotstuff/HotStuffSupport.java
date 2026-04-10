package pt.ulisboa.depchain.server.consensus.hotstuff;

import com.google.protobuf.ByteString;

import pt.ulisboa.depchain.proto.AuthOpcode;
import pt.ulisboa.depchain.proto.AuthenticatedDataEnvelope;
import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ConsensusMessageType;
import pt.ulisboa.depchain.proto.DpchPacket;
import pt.ulisboa.depchain.proto.DpchPacketType;
import pt.ulisboa.depchain.proto.GenesisCommand;
import pt.ulisboa.depchain.proto.Message;
import pt.ulisboa.depchain.proto.NoOpCommand;
import pt.ulisboa.depchain.proto.Node;
import pt.ulisboa.depchain.proto.NodeCommand;
import pt.ulisboa.depchain.proto.ProposalMessage;
import pt.ulisboa.depchain.proto.QuorumCertificate;
import pt.ulisboa.depchain.proto.TransactionRequest;
import pt.ulisboa.depchain.shared.validation.ValidationUtils;

public final class HotStuffSupport {
  private static final String HASH_PLACEHOLDER = "a".repeat(64);
  public static final String NO_OP_VALUE = "no-op";
  public static final String GENESIS_VALUE = "GENESIS";
  public static final NodeCommand NO_OP_COMMAND = NodeCommand.newBuilder().setNoOp(NoOpCommand.getDefaultInstance()).build();
  public static final NodeCommand GENESIS_COMMAND = NodeCommand.newBuilder().setGenesis(GenesisCommand.getDefaultInstance()).build();
  public static final Node GENESIS_NODE = Node.newBuilder().setParentNodeHash("0").setNodeHash("0").setViewNumber(0).setCommand(GENESIS_COMMAND).build();

  private HotStuffSupport() {
  }

  public static boolean isNoOp(NodeCommand command) {
    ValidationUtils.requireNonNull(command, "command");
    return command.hasNoOp();
  }

  public static String commandSummary(NodeCommand command) {
    ValidationUtils.requireNonNull(command, "command");
    if (command.hasTransactionBatch()) {
      if (command.getTransactionBatch().getClientRequestsCount() == 1) {
        var transaction = command.getTransactionBatch().getClientRequests(0).getTransaction();
        return switch (transaction.getType()) {
          case TRANSACTION_TYPE_TRANSFER -> "dep_transfer to=%s amount=%d nonce=%d".formatted(transaction.getTo(), transaction.getAmount(), transaction.getNonce());
          case TRANSACTION_TYPE_CONTRACT_CALL -> "contract_call to=%s amount=%d nonce=%d inputBytes=%d"
              .formatted(transaction.getTo(), transaction.getAmount(), transaction.getNonce(), transaction.hasInput() ? transaction.getInput().size() : 0);
          case TRANSACTION_TYPE_IST_COIN_TRANSFER -> "ist_transfer to=%s amount=%d nonce=%d".formatted(transaction.getTo(), transaction.getAmount(), transaction.getNonce());
          default -> "transaction type=%s to=%s nonce=%d".formatted(transaction.getType(), transaction.getTo(), transaction.getNonce());
        };
      }
      long topGasPrice = command.getTransactionBatch().getClientRequestsList().stream().filter(ClientRequest::hasTransaction).map(ClientRequest::getTransaction)
          .mapToLong(TransactionRequest::getGasPrice).max().orElse(0L);
      return "batch txs=%d topGasPrice=%d".formatted(command.getTransactionBatch().getClientRequestsCount(), topGasPrice);
    }
    if (command.hasNoOp()) {
      return NO_OP_VALUE;
    }

    if (command.hasGenesis()) {
      return GENESIS_VALUE;
    }

    throw new IllegalArgumentException("Node command body is not set");
  }

  public static boolean extendsNode(Node node, Node other) {
    ValidationUtils.requireAllNonNull(ValidationUtils.named("node", node), ValidationUtils.named("other", other));
    return node.getParentNodeHash().equals(other.getNodeHash());
  }

  public static boolean isSameNode(Node left, Node right) {
    if (left == null || right == null) {
      return left == right;
    }

    return left.getNodeHash().equals(right.getNodeHash());
  }

  public static boolean isAuxiliaryMessage(Message message) {
    ValidationUtils.requireNonNull(message, "message");
    return switch (message.getBodyCase()) {
      case VOTE, COMMITMENT, THRESHOLD_CONTEXT, FETCH_NODE_REQUEST, FETCH_NODE_RESPONSE -> true;
      default -> false;
    };
  }

  public static int estimatePrepareProposalDatagramSize(NodeCommand command, QuorumCertificate justifyQc, int viewNumber, int replicaSenderId) {
    ValidationUtils.requireAllNonNull(ValidationUtils.named("command", command), ValidationUtils.named("justifyQc", justifyQc));
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");
    ValidationUtils.requireNonNegativeInt(replicaSenderId, "replicaSenderId");

    Node proposedNode = Node.newBuilder().setParentNodeHash(HASH_PLACEHOLDER).setNodeHash(HASH_PLACEHOLDER).setViewNumber(viewNumber).setCommand(command).build();
    Message proposalMessage = Message.newBuilder().setViewNumber(viewNumber).setReplicaSenderId(replicaSenderId).setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE)
        .setProposal(ProposalMessage.newBuilder().setProposedNode(proposedNode).setJustifyQc(justifyQc)).build();

    byte[] proposalPayload = proposalMessage.toByteArray();
    AuthenticatedDataEnvelope authenticatedEnvelope = AuthenticatedDataEnvelope.newBuilder().setAuthOpcode(AuthOpcode.AUTH_OPCODE_DATA)
        .setApplicationPayload(ByteString.copyFrom(proposalPayload)).setNonce(1L).setHmac(ByteString.copyFrom(new byte[32])).build();
    DpchPacket packet = DpchPacket.newBuilder().setConnectionId(1L).setSequenceNumber(1).setPacketType(DpchPacketType.DPCH_PACKET_TYPE_DATA)
        .setPayload(authenticatedEnvelope.toByteString()).build();
    return packet.toByteArray().length;
  }
}
