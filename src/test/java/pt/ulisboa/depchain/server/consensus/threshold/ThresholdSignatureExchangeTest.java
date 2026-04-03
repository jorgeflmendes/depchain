package pt.ulisboa.depchain.server.consensus.threshold;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;

import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.CommitmentMessage;
import pt.ulisboa.depchain.proto.ConsensusMessageType;
import pt.ulisboa.depchain.proto.Message;
import pt.ulisboa.depchain.proto.Node;
import pt.ulisboa.depchain.proto.NodeCommand;
import pt.ulisboa.depchain.proto.ThresholdContextMessage;
import pt.ulisboa.depchain.proto.ThresholdSignatureContext;
import pt.ulisboa.depchain.proto.TransactionBatchNodeCommand;
import pt.ulisboa.depchain.proto.TransactionRequest;
import pt.ulisboa.depchain.proto.TransactionType;
import pt.ulisboa.depchain.proto.VoteMessage;
import pt.ulisboa.depchain.server.consensus.ConsensusTimeoutException;
import pt.ulisboa.depchain.server.consensus.hotstuff.HotStuffCryptoPayloads;
import pt.ulisboa.depchain.server.consensus.hotstuff.HotStuffSupport;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.crypto.ClientRequestSignaturePayloadUtil;
import pt.ulisboa.depchain.shared.crypto.CryptoUtil;
import pt.ulisboa.depchain.shared.crypto.key.PrivateKeyLoader;
import pt.ulisboa.depchain.testsupport.TestKeyMaterialSupport;

class ThresholdSignatureExchangeTest {
  private static final String TEST_RECIPIENT_ADDRESS = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

  @BeforeAll
  static void ensureKeyMaterial() throws Exception {
    TestKeyMaterialSupport.ensureKeyMaterial(configPath());
  }

  @Test
  void collectCommitmentsIgnoresDuplicateSenderAndWrongPhaseMessages() throws Exception {
    ConfigParser config = ConfigParser.load(configPath());
    ThresholdSignatureExchange exchange = new ThresholdSignatureExchange(config, 0, 100L);
    Node node = createNode(1, "commitments", 801L);
    LinkedBlockingDeque<Message> queue = new LinkedBlockingDeque<>();

    queue.add(commitmentMessage(1, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 1, node, 0x11));
    queue.add(commitmentMessage(1, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 1, node, 0x22));
    queue.add(commitmentMessage(2, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT, 1, node, 0x33));
    queue.add(commitmentMessage(2, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 1, node, 0x44));

    List<ThresholdSignatureExchange.RemoteCommitment> commitments = exchange
        .collectCommitments(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 1, node, 2, queue, System.nanoTime() + 1_000_000_000L);

    assertEquals(2, commitments.size());
    assertEquals(List.of(1, 2), commitments.stream().map(ThresholdSignatureExchange.RemoteCommitment::senderId).toList());
    assertFalse(queue.isEmpty());
    assertTrue(queue.peekFirst().getMessageType() == ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT);
  }

  @Test
  void collectSignatureSharesRejectsMismatchedAggregatedCommitment() throws Exception {
    ConfigParser config = ConfigParser.load(configPath());
    ThresholdSignatureExchange exchange = new ThresholdSignatureExchange(config, 0, 100L);
    Node node = createNode(2, "shares", 802L);
    LinkedBlockingDeque<Message> queue = new LinkedBlockingDeque<>();
    byte[] expectedCommitment = repeatedByteArray(0x55);

    queue.add(signatureShareMessage(1, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 2, node, repeatedByteArray(0x31), repeatedByteArray(0x12)));
    queue.add(signatureShareMessage(2, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 2, node, repeatedByteArray(0x32), expectedCommitment));
    queue.add(signatureShareMessage(3, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 2, node, repeatedByteArray(0x33), expectedCommitment));

    List<byte[]> shares = exchange.collectSignatureShares(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 2, node, expectedCommitment, java.util.Set
        .of(2, 3), queue, System.nanoTime() + 1_000_000_000L);

    assertEquals(2, shares.size());
    assertEquals(0x32, shares.get(0)[0] & 0xff);
    assertEquals(0x33, shares.get(1)[0] & 0xff);
    assertFalse(queue.isEmpty());
    assertEquals(1, queue.peekFirst().getReplicaSenderId());
  }

  @Test
  void collectSignatureSharesIgnoresDuplicateAndUnexpectedSenders() throws Exception {
    ConfigParser config = ConfigParser.load(configPath());
    ThresholdSignatureExchange exchange = new ThresholdSignatureExchange(config, 0, 100L);
    Node node = createNode(2, "shares-dedup", 806L);
    LinkedBlockingDeque<Message> queue = new LinkedBlockingDeque<>();
    byte[] expectedCommitment = repeatedByteArray(0x66);

    queue.add(signatureShareMessage(1, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 2, node, repeatedByteArray(0x21), expectedCommitment));
    queue.add(signatureShareMessage(1, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 2, node, repeatedByteArray(0x22), expectedCommitment));
    queue.add(signatureShareMessage(4, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 2, node, repeatedByteArray(0x23), expectedCommitment));
    queue.add(signatureShareMessage(2, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 2, node, repeatedByteArray(0x24), expectedCommitment));

    List<byte[]> shares = exchange.collectSignatureShares(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 2, node, expectedCommitment, java.util.Set
        .of(1, 2), queue, System.nanoTime() + 1_000_000_000L);

    assertEquals(2, shares.size());
    assertEquals(0x21, shares.get(0)[0] & 0xff);
    assertEquals(0x24, shares.get(1)[0] & 0xff);
    assertFalse(queue.isEmpty());
    assertEquals(4, queue.peekFirst().getReplicaSenderId());
  }

  @Test
  void waitForContextSkipsWrongNodeUntilMatchingContextArrives() throws Exception {
    ConfigParser config = ConfigParser.load(configPath());
    ThresholdSignatureExchange exchange = new ThresholdSignatureExchange(config, 0, 100L);
    Node expectedNode = createNode(3, "expected", 803L);
    Node wrongNode = createNode(3, "wrong", 804L);
    LinkedBlockingDeque<Message> queue = new LinkedBlockingDeque<>();
    byte[] aggregatedCommitment = repeatedByteArray(0x77);

    queue.add(thresholdContextMessage(1, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 3, wrongNode, aggregatedCommitment, List.of(0, 1, 2)));
    queue.add(thresholdContextMessage(1, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 3, expectedNode, aggregatedCommitment, List.of(0, 1, 2)));

    ThresholdSignatureExchange.ThresholdContext context = exchange.waitForContext(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 3, 1, expectedNode, queue);

    assertEquals(List.of(0, 1, 2), context.participantIndexes().stream().sorted().toList());
    assertEquals(0x77, context.aggregatedCommitment()[0] & 0xff);
    assertFalse(queue.isEmpty());
    assertEquals(wrongNode.getNodeHash(), queue.peekFirst().getThresholdContext().getVotedNode().getNodeHash());
  }

  @Test
  void waitForContextSkipsWrongLeaderUntilExpectedLeaderArrives() throws Exception {
    ConfigParser config = ConfigParser.load(configPath());
    ThresholdSignatureExchange exchange = new ThresholdSignatureExchange(config, 0, 100L);
    Node expectedNode = createNode(3, "expected-leader", 807L);
    LinkedBlockingDeque<Message> queue = new LinkedBlockingDeque<>();
    byte[] aggregatedCommitment = repeatedByteArray(0x78);

    queue.add(thresholdContextMessage(2, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 3, expectedNode, aggregatedCommitment, List.of(0, 1, 2)));
    queue.add(thresholdContextMessage(1, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 3, expectedNode, aggregatedCommitment, List.of(0, 1, 2)));

    ThresholdSignatureExchange.ThresholdContext context = exchange.waitForContext(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 3, 1, expectedNode, queue);

    assertEquals(List.of(0, 1, 2), context.participantIndexes().stream().sorted().toList());
    assertEquals(0x78, context.aggregatedCommitment()[0] & 0xff);
    assertFalse(queue.isEmpty());
    assertEquals(2, queue.peekFirst().getReplicaSenderId());
  }

  @Test
  void collectCommitmentsTimesOutWithoutEnoughDistinctSenders() throws Exception {
    ConfigParser config = ConfigParser.load(configPath());
    ThresholdSignatureExchange exchange = new ThresholdSignatureExchange(config, 0, 50L);
    Node node = createNode(4, "timeout", 805L);
    LinkedBlockingDeque<Message> queue = new LinkedBlockingDeque<>();

    queue.add(commitmentMessage(1, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 4, node, 0x11));
    queue.add(commitmentMessage(1, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 4, node, 0x22));

    org.junit.jupiter.api.Assertions.assertThrows(ConsensusTimeoutException.class, () -> exchange
        .collectCommitments(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 4, node, 2, queue, System.nanoTime() + 50_000_000L));
  }

  @Test
  void collectSignatureSharesTimesOutWithoutEnoughDistinctExpectedSenders() throws Exception {
    ConfigParser config = ConfigParser.load(configPath());
    ThresholdSignatureExchange exchange = new ThresholdSignatureExchange(config, 0, 50L);
    Node node = createNode(5, "share-timeout", 808L);
    LinkedBlockingDeque<Message> queue = new LinkedBlockingDeque<>();
    byte[] expectedCommitment = repeatedByteArray(0x79);

    queue.add(signatureShareMessage(1, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 5, node, repeatedByteArray(0x41), expectedCommitment));
    queue.add(signatureShareMessage(1, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 5, node, repeatedByteArray(0x42), expectedCommitment));
    queue.add(signatureShareMessage(4, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 5, node, repeatedByteArray(0x43), expectedCommitment));

    org.junit.jupiter.api.Assertions.assertThrows(ConsensusTimeoutException.class, () -> exchange
        .collectSignatureShares(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 5, node, expectedCommitment, java.util.Set.of(1, 2), queue, System.nanoTime() + 50_000_000L));
  }

  private static Message commitmentMessage(int senderId, ConsensusMessageType type, int viewNumber, Node node, int firstByte) {
    return Message.newBuilder().setViewNumber(viewNumber).setReplicaSenderId(senderId).setMessageType(type)
        .setCommitment(CommitmentMessage.newBuilder().setVotedNode(node).setThresholdSignatureCommitment(ByteString.copyFrom(repeatedByteArray(firstByte)))).build();
  }

  private static Message signatureShareMessage(int senderId, ConsensusMessageType type, int viewNumber, Node node, byte[] share, byte[] aggregatedCommitment) {
    return Message.newBuilder().setViewNumber(viewNumber).setReplicaSenderId(senderId).setMessageType(type).setVote(VoteMessage.newBuilder().setVotedNode(node)
        .setThresholdSignatureShare(ByteString.copyFrom(share)).setAggregatedCommitment(ByteString.copyFrom(aggregatedCommitment))).build();
  }

  private static Message thresholdContextMessage(int senderId, ConsensusMessageType type, int viewNumber, Node node, byte[] aggregatedCommitment, List<Integer> participantIndexes) {
    return Message.newBuilder().setViewNumber(viewNumber).setReplicaSenderId(senderId).setMessageType(type)
        .setThresholdContext(ThresholdContextMessage.newBuilder().setVotedNode(node).setThresholdSignatureContext(ThresholdSignatureContext.newBuilder()
            .setAggregatedCommitment(ByteString.copyFrom(aggregatedCommitment)).addAllParticipantReplicaIndexes(participantIndexes)))
        .build();
  }

  private static byte[] repeatedByteArray(int value) {
    byte[] bytes = new byte[32];
    java.util.Arrays.fill(bytes, (byte) value);
    return bytes;
  }

  private static Node createNode(int viewNumber, String value, long requestId) {
    NodeCommand command = NodeCommand.newBuilder()
        .setTransactionBatch(TransactionBatchNodeCommand.newBuilder().addClientRequests(signedTransferRequest(requestId, value, viewNumber))).build();
    String nodeHash = CryptoUtil.sha256Hex(HotStuffCryptoPayloads.nodeHashPayload(HotStuffSupport.GENESIS_NODE.getNodeHash(), viewNumber, command));
    return Node.newBuilder().setParentNodeHash(HotStuffSupport.GENESIS_NODE.getNodeHash()).setNodeHash(nodeHash).setViewNumber(viewNumber).setCommand(command).build();
  }

  private static ClientRequest signedTransferRequest(long requestId, String value, long nonce) {
    try {
      ConfigParser config = ConfigParser.load(configPath());
      long clientSenderId = config.client().senderId();
      PrivateKey clientPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config);
      long amount = Math.max(1L, value.length());
      byte[] signature = CryptoUtil.signEcdsa(ClientRequestSignaturePayloadUtil
          .signedTransactionRequestPayload(clientSenderId, requestId, TransactionType.TRANSACTION_TYPE_TRANSFER, TEST_RECIPIENT_ADDRESS, amount, nonce, 21_000L, 1L), clientPrivateKey);
      return ClientRequest.newBuilder()
          .setTransaction(TransactionRequest.newBuilder().setRequestKey(ClientRequestKey.newBuilder().setClientSenderId(clientSenderId).setRequestId(requestId))
              .setType(TransactionType.TRANSACTION_TYPE_TRANSFER).setTo(TEST_RECIPIENT_ADDRESS).setAmount(amount).setNonce(nonce).setGasLimit(21_000L).setGasPrice(1L)
              .setSignature(ByteString.copyFrom(signature)))
          .build();
    } catch (Exception exception) {
      throw new IllegalStateException("Could not create signed transfer request", exception);
    }
  }

  private static Path configPath() {
    try {
      return TestKeyMaterialSupport.isolatedConfigPath("ThresholdSignatureExchangeTest");
    } catch (IOException exception) {
      throw new IllegalStateException("Could not prepare isolated config for ThresholdSignatureExchangeTest", exception);
    }
  }
}
