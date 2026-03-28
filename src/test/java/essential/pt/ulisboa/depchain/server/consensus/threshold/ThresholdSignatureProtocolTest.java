package pt.ulisboa.depchain.server.consensus.threshold;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.weavechain.curve25519.Scalar;

import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.CommitmentMessage;
import pt.ulisboa.depchain.proto.ConsensusMessageType;
import pt.ulisboa.depchain.proto.Message;
import pt.ulisboa.depchain.proto.Node;
import pt.ulisboa.depchain.proto.NodeCommand;
import pt.ulisboa.depchain.proto.QuorumCertificate;
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
import pt.ulisboa.depchain.shared.crypto.ThresholdCryptoUtil;
import pt.ulisboa.depchain.shared.crypto.key.PrivateKeyLoader;
import pt.ulisboa.depchain.shared.crypto.key.ThresholdKeyLoader;
import pt.ulisboa.depchain.testsupport.TestKeyMaterialSupport;

class ThresholdSignatureProtocolTest {
  private static final String TEST_RECIPIENT_ADDRESS = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

  @BeforeAll
  static void ensureKeyMaterial() throws Exception {
    TestKeyMaterialSupport.ensureKeyMaterial(configPath());
  }

  @Test
  void verifyQcAcceptsGenesisAndRejectsTamperedGenesis() throws Exception {
    ThresholdSignatureProtocol protocol = protocol();
    QuorumCertificate genesisQc = protocol.genesisQC();
    QuorumCertificate tamperedGenesisQc = genesisQc.toBuilder().setViewNumber(0).build();

    assertTrue(protocol.verifyQC(genesisQc));
    assertFalse(protocol.verifyQC(tamperedGenesisQc));
  }

  @Test
  void verifyQcRejectsTamperedFieldsAfterValidSignature() throws Exception {
    ConfigParser config = ConfigParser.load(configPath());
    ThresholdSignatureProtocol protocol = protocol();
    byte[] publicThresholdKey = ThresholdKeyLoader.loadReplicaThresholdPublicKey(config, 0L);
    Scalar share0 = ThresholdKeyLoader.loadReplicaThresholdPrivateShare(config, 0L);
    Scalar share1 = ThresholdKeyLoader.loadReplicaThresholdPrivateShare(config, 1L);
    Scalar share2 = ThresholdKeyLoader.loadReplicaThresholdPrivateShare(config, 2L);
    Node node = createNode(1, "command-one", 77L);

    QuorumCertificate validQc = signedQc(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 1, node, publicThresholdKey, List.of(share0, share1, share2), Set.of(0, 1, 2), config
        .system().n(), config.system().n() - config.system().f());
    QuorumCertificate tamperedView = validQc.toBuilder().setViewNumber(2).build();
    QuorumCertificate tamperedType = validQc.toBuilder().setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_COMMIT).build();
    QuorumCertificate tamperedNode = validQc.toBuilder().setCertifiedNode(createNode(1, "command-two", 78L)).build();

    assertTrue(protocol.verifyQC(validQc));
    assertFalse(protocol.verifyQC(tamperedView));
    assertFalse(protocol.verifyQC(tamperedType));
    assertFalse(protocol.verifyQC(tamperedNode));
  }

  @Test
  void verifyQcRejectsRandomSignatureWithValidShape() throws Exception {
    ThresholdSignatureProtocol protocol = protocol();
    Node node = createNode(2, "command-three", 79L);
    QuorumCertificate invalidQc = QuorumCertificate.newBuilder().setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT).setViewNumber(2).setCertifiedNode(node)
        .setQuorumSignature(ByteString.copyFrom(new byte[64])).build();

    assertFalse(protocol.verifyQC(invalidQc));
  }

  @Test
  void verifyQcRejectsMissingCertifiedNodeOrMissingSignature() throws Exception {
    ThresholdSignatureProtocol protocol = protocol();
    Node node = createNode(3, "command-four", 80L);

    QuorumCertificate missingNode = QuorumCertificate.newBuilder().setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE).setViewNumber(3)
        .setQuorumSignature(ByteString.copyFrom(new byte[64])).build();
    QuorumCertificate missingSignature = QuorumCertificate.newBuilder().setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE).setViewNumber(3).setCertifiedNode(node)
        .build();

    assertFalse(protocol.verifyQC(missingNode));
    assertFalse(protocol.verifyQC(missingSignature));
  }

  @Test
  void verifyQcRejectsSignatureProducedUnderDifferentThresholdPublicKey() throws Exception {
    ThresholdSignatureProtocol protocol = protocol();
    ThresholdCryptoUtil.ThresholdConfig foreignConfig = ThresholdCryptoUtil.createThresholdConfig(4, 3);
    Node node = createNode(4, "command-five", 81L);
    QuorumCertificate foreignQc = signedQc(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 4, node, foreignConfig.publicKey(), List
        .of(foreignConfig.privateShare(0), foreignConfig.privateShare(1), foreignConfig.privateShare(2)), Set.of(0, 1, 2), 4, 3);

    assertFalse(protocol.verifyQC(foreignQc));
  }

  @Test
  void buildQcTimesOutWhenCollectedSubsetsNeverProduceValidSignature() throws Exception {
    ConfigParser config = ConfigParser.load(configPath());
    ThresholdSignatureProtocol protocol = protocol();
    Scalar share1 = ThresholdKeyLoader.loadReplicaThresholdPrivateShare(config, 1L);
    Scalar share2 = ThresholdKeyLoader.loadReplicaThresholdPrivateShare(config, 2L);
    Scalar share3 = ThresholdKeyLoader.loadReplicaThresholdPrivateShare(config, 3L);
    Node node = createNode(5, "fallback-qc", 82L);
    byte[] payload = HotStuffCryptoPayloads.votePayload(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 5, node);
    LinkedBlockingDeque<Message> queue = new LinkedBlockingDeque<>();

    ThresholdCryptoUtil.ThresholdNonceShare nonce1 = ThresholdCryptoUtil.thresholdNonceShare(payload, share1);
    ThresholdCryptoUtil.ThresholdNonceShare nonce2 = ThresholdCryptoUtil.thresholdNonceShare(payload, share2);
    ThresholdCryptoUtil.ThresholdNonceShare nonce3 = ThresholdCryptoUtil.thresholdNonceShare(payload, share3);
    byte[] aggregatedCommitment12 = repeatedByteArray(0x51);
    byte[] aggregatedCommitment13 = repeatedByteArray(0x52);

    queue.add(commitmentMessage(1, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 5, node, nonce1.commitment()));
    queue.add(commitmentMessage(2, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 5, node, nonce2.commitment()));
    queue.add(commitmentMessage(3, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 5, node, nonce3.commitment()));

    queue.add(signatureShareMessage(1, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 5, node, repeatedByteArray(0x61), aggregatedCommitment12));
    queue.add(signatureShareMessage(2, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 5, node, repeatedByteArray(0x62), aggregatedCommitment12));
    queue.add(signatureShareMessage(1, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 5, node, repeatedByteArray(0x63), aggregatedCommitment13));
    queue.add(signatureShareMessage(3, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 5, node, repeatedByteArray(0x64), aggregatedCommitment13));

    org.junit.jupiter.api.Assertions.assertThrows(ConsensusTimeoutException.class, () -> protocol.buildQC(5, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, node, queue));
  }

  private static ThresholdSignatureProtocol protocol() throws Exception {
    ConfigParser config = ConfigParser.load(configPath());
    return new ThresholdSignatureProtocol(0, config, ThresholdKeyLoader.loadReplicaThresholdPrivateShare(config, 0L), ThresholdKeyLoader.loadReplicaThresholdPublicKey(config, 0L));
  }

  private static Message commitmentMessage(int senderId, ConsensusMessageType type, int viewNumber, Node node, byte[] commitment) {
    return Message.newBuilder().setViewNumber(viewNumber).setReplicaSenderId(senderId).setMessageType(type)
        .setCommitment(CommitmentMessage.newBuilder().setVotedNode(node).setThresholdSignatureCommitment(ByteString.copyFrom(commitment))).build();
  }

  private static Message signatureShareMessage(int senderId, ConsensusMessageType type, int viewNumber, Node node, byte[] share, byte[] aggregatedCommitment) {
    return Message.newBuilder().setViewNumber(viewNumber).setReplicaSenderId(senderId).setMessageType(type).setVote(VoteMessage.newBuilder().setVotedNode(node)
        .setThresholdSignatureShare(ByteString.copyFrom(share)).setAggregatedCommitment(ByteString.copyFrom(aggregatedCommitment))).build();
  }

  private static byte[] repeatedByteArray(int value) {
    byte[] bytes = new byte[32];
    java.util.Arrays.fill(bytes, (byte) value);
    return bytes;
  }

  private static QuorumCertificate signedQc(ConsensusMessageType type, int viewNumber, Node node, byte[] publicThresholdKey, List<Scalar> shares, Set<Integer> participantIndexes, int totalReplicas, int threshold)
      throws Exception {
    byte[] payload = HotStuffCryptoPayloads.votePayload(type, viewNumber, node);
    ThresholdCryptoUtil.ThresholdNonceShare nonce0 = ThresholdCryptoUtil.thresholdNonceShare(payload, shares.get(0));
    ThresholdCryptoUtil.ThresholdNonceShare nonce1 = ThresholdCryptoUtil.thresholdNonceShare(payload, shares.get(1));
    ThresholdCryptoUtil.ThresholdNonceShare nonce2 = ThresholdCryptoUtil.thresholdNonceShare(payload, shares.get(2));
    byte[] aggregatedCommitment = ThresholdCryptoUtil
        .thresholdAggregateCommitments(totalReplicas, threshold, List.of(nonce0.commitment(), nonce1.commitment(), nonce2.commitment()));

    byte[] sig0 = ThresholdCryptoUtil.thresholdPartialSign(payload, shares.get(0), nonce0, new ThresholdCryptoUtil.ThresholdPartialSignContext(0, totalReplicas, threshold,
        participantIndexes, publicThresholdKey, aggregatedCommitment));
    byte[] sig1 = ThresholdCryptoUtil.thresholdPartialSign(payload, shares.get(1), nonce1, new ThresholdCryptoUtil.ThresholdPartialSignContext(1, totalReplicas, threshold,
        participantIndexes, publicThresholdKey, aggregatedCommitment));
    byte[] sig2 = ThresholdCryptoUtil.thresholdPartialSign(payload, shares.get(2), nonce2, new ThresholdCryptoUtil.ThresholdPartialSignContext(2, totalReplicas, threshold,
        participantIndexes, publicThresholdKey, aggregatedCommitment));
    byte[] quorumSignature = ThresholdCryptoUtil.thresholdCombinePartialSignatures(totalReplicas, threshold, aggregatedCommitment, List.of(sig0, sig1, sig2));

    return QuorumCertificate.newBuilder().setMessageType(type).setViewNumber(viewNumber).setCertifiedNode(node).setQuorumSignature(ByteString.copyFrom(quorumSignature)).build();
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
      return TestKeyMaterialSupport.isolatedConfigPath("ThresholdSignatureProtocolTest");
    } catch (IOException exception) {
      throw new IllegalStateException("Could not prepare isolated config for ThresholdSignatureProtocolTest", exception);
    }
  }
}
