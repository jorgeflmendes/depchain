package pt.ulisboa.depchain.server.consensus.hotstuff;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.weavechain.curve25519.Scalar;

import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.ConsensusMessageType;
import pt.ulisboa.depchain.proto.Node;
import pt.ulisboa.depchain.proto.NodeCommand;
import pt.ulisboa.depchain.proto.QuorumCertificate;
import pt.ulisboa.depchain.proto.TransactionBatchNodeCommand;
import pt.ulisboa.depchain.proto.TransactionRequest;
import pt.ulisboa.depchain.proto.TransactionType;
import pt.ulisboa.depchain.server.execution.EvmService;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.crypto.ClientRequestSignaturePayloadUtil;
import pt.ulisboa.depchain.shared.crypto.CryptoUtil;
import pt.ulisboa.depchain.shared.crypto.ThresholdCryptoUtil;
import pt.ulisboa.depchain.shared.crypto.key.PrivateKeyLoader;
import pt.ulisboa.depchain.shared.crypto.key.PublicKeyLoader;
import pt.ulisboa.depchain.shared.crypto.key.ThresholdKeyLoader;
import pt.ulisboa.depchain.testsupport.TestKeyMaterialSupport;

class HotStuffManagerInvariantTest {
  private static final String TEST_RECIPIENT_ADDRESS = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

  @BeforeAll
  static void ensureKeyMaterial() throws Exception {
    TestKeyMaterialSupport.ensureKeyMaterial(configPath());
  }

  @Test
  void safeNodeRejectsConflictingBranchWhenJustifyDoesNotBeatLockView() throws Exception {
    HotStuffManager manager = createHotStuffManager();
    Node lockedNode = createNode(1, HotStuffSupport.GENESIS_NODE.getNodeHash(), 501L, "locked");
    Node conflictingNode = createNode(2, HotStuffSupport.GENESIS_NODE.getNodeHash(), 502L, "conflict");
    QuorumCertificate lockedQc = unsignedQc(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT, 1, lockedNode);
    QuorumCertificate justifyQc = unsignedQc(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 1, conflictingNode);

    putKnownNode(manager, lockedNode);
    putKnownNode(manager, conflictingNode);
    setField(manager, "lockedQC", lockedQc);

    assertFalse(invokeSafeNode(manager, conflictingNode, justifyQc));
  }

  @Test
  void safeNodeAcceptsConflictingBranchWhenJustifyViewBeatsLock() throws Exception {
    HotStuffManager manager = createHotStuffManager();
    Node lockedNode = createNode(1, HotStuffSupport.GENESIS_NODE.getNodeHash(), 601L, "locked");
    Node conflictingNode = createNode(3, HotStuffSupport.GENESIS_NODE.getNodeHash(), 602L, "conflict");
    QuorumCertificate lockedQc = unsignedQc(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT, 1, lockedNode);
    QuorumCertificate justifyQc = unsignedQc(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 2, conflictingNode);

    putKnownNode(manager, lockedNode);
    putKnownNode(manager, conflictingNode);
    setField(manager, "lockedQC", lockedQc);

    assertTrue(invokeSafeNode(manager, conflictingNode, justifyQc));
  }

  @Test
  void safeNodeAcceptsDescendantOfLockedBranchEvenWhenJustifyDoesNotBeatLockView() throws Exception {
    HotStuffManager manager = createHotStuffManager();
    Node lockedNode = createNode(1, HotStuffSupport.GENESIS_NODE.getNodeHash(), 651L, "locked");
    Node descendantNode = createNode(2, lockedNode.getNodeHash(), 652L, "descendant");
    QuorumCertificate lockedQc = unsignedQc(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT, 2, lockedNode);
    QuorumCertificate justifyQc = unsignedQc(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 1, lockedNode);

    putKnownNode(manager, lockedNode);
    putKnownNode(manager, descendantNode);
    setField(manager, "lockedQC", lockedQc);

    assertTrue(invokeSafeNode(manager, descendantNode, justifyQc));
  }

  @Test
  void validPrepareJustifyAcceptsOnlyGenesisDecideOrSignedPrepare() throws Exception {
    ConfigParser config = ConfigParser.load(configPath());
    HotStuffManager manager = createHotStuffManager();
    byte[] publicThresholdKey = ThresholdKeyLoader.loadReplicaThresholdPublicKey(config, 0L);
    Scalar share0 = ThresholdKeyLoader.loadReplicaThresholdPrivateShare(config, 0L);
    Scalar share1 = ThresholdKeyLoader.loadReplicaThresholdPrivateShare(config, 1L);
    Scalar share2 = ThresholdKeyLoader.loadReplicaThresholdPrivateShare(config, 2L);
    Node preparedNode = createNode(2, HotStuffSupport.GENESIS_NODE.getNodeHash(), 701L, "prepared");

    QuorumCertificate genesisQc = genesisQc(manager);
    QuorumCertificate prepareQc = signedQc(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE, 2, preparedNode, publicThresholdKey, List.of(share0, share1, share2), Set
        .of(0, 1, 2), config.system().n(), config.system().n() - config.system().f());
    QuorumCertificate commitQc = signedQc(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_COMMIT, 2, preparedNode, publicThresholdKey, List.of(share0, share1, share2), Set
        .of(0, 1, 2), config.system().n(), config.system().n() - config.system().f());

    assertTrue(invokeIsValidPrepareJustifyQc(manager, genesisQc));
    assertTrue(invokeIsValidPrepareJustifyQc(manager, prepareQc));
    assertFalse(invokeIsValidPrepareJustifyQc(manager, commitQc));
  }

  @Test
  void validPrepareJustifyRejectsGenesisLikeQcWithWrongNodeOrWrongType() throws Exception {
    HotStuffManager manager = createHotStuffManager();
    QuorumCertificate wrongGenesisType = QuorumCertificate.newBuilder().setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PREPARE).setViewNumber(-1)
        .setCertifiedNode(HotStuffSupport.GENESIS_NODE).build();
    QuorumCertificate wrongGenesisNode = QuorumCertificate.newBuilder().setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_DECIDE).setViewNumber(-1)
        .setCertifiedNode(createNode(0, HotStuffSupport.GENESIS_NODE.getNodeHash(), 801L, "fake-genesis")).build();

    assertFalse(invokeIsValidPrepareJustifyQc(manager, wrongGenesisType));
    assertFalse(invokeIsValidPrepareJustifyQc(manager, wrongGenesisNode));
  }

  private static HotStuffManager createHotStuffManager() throws Exception {
    ConfigParser config = ConfigParser.load(configPath());
    PublicKey clientPublicKey = PublicKeyLoader.loadClientPublicKey(config);
    return new HotStuffManager(0, config, ThresholdKeyLoader.loadReplicaThresholdPrivateShare(config, 0L), ThresholdKeyLoader.loadReplicaThresholdPublicKey(config, 0L),
        Map.of(config.client().senderId(), clientPublicKey), new EvmService(), null, node -> {
        });
  }

  private static QuorumCertificate genesisQc(HotStuffManager manager) throws Exception {
    Object thresholdProtocol = getField(manager, "thresholdProtocol");
    Method method = thresholdProtocol.getClass().getDeclaredMethod("genesisQC");
    method.setAccessible(true);
    return (QuorumCertificate) method.invoke(thresholdProtocol);
  }

  private static boolean invokeSafeNode(HotStuffManager manager, Node node, QuorumCertificate qc) throws Exception {
    Method method = HotStuffManager.class.getDeclaredMethod("safeNode", Node.class, QuorumCertificate.class);
    method.setAccessible(true);
    return (boolean) method.invoke(manager, node, qc);
  }

  private static boolean invokeIsValidPrepareJustifyQc(HotStuffManager manager, QuorumCertificate qc) throws Exception {
    Method method = HotStuffManager.class.getDeclaredMethod("isValidPrepareJustifyQc", QuorumCertificate.class);
    method.setAccessible(true);
    return (boolean) method.invoke(manager, qc);
  }

  private static void putKnownNode(HotStuffManager manager, Node node) throws Exception {
    @SuppressWarnings("unchecked")
    Map<String, Node> blockTree = new HashMap<>((Map<String, Node>) getField(manager, "blockTree"));
    blockTree.put(node.getNodeHash(), node);
    setField(manager, "blockTree", blockTree);
  }

  private static Object getField(HotStuffManager manager, String fieldName) throws Exception {
    Field field = HotStuffManager.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.get(manager);
  }

  private static void setField(HotStuffManager manager, String fieldName, Object value) throws Exception {
    Field field = HotStuffManager.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(manager, value);
  }

  private static QuorumCertificate unsignedQc(ConsensusMessageType type, int viewNumber, Node node) {
    return QuorumCertificate.newBuilder().setMessageType(type).setViewNumber(viewNumber).setCertifiedNode(node).build();
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

  private static Node createNode(int viewNumber, String parentHash, long requestId, String value) {
    NodeCommand command = NodeCommand.newBuilder()
        .setTransactionBatch(TransactionBatchNodeCommand.newBuilder().addClientRequests(signedTransferRequest(requestId, value, viewNumber))).build();
    String nodeHash = CryptoUtil.sha256Hex(HotStuffCryptoPayloads.nodeHashPayload(parentHash, viewNumber, command));
    return Node.newBuilder().setParentNodeHash(parentHash).setNodeHash(nodeHash).setViewNumber(viewNumber).setCommand(command).build();
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
      return TestKeyMaterialSupport.isolatedConfigPath("HotStuffManagerInvariantTest");
    } catch (IOException exception) {
      throw new IllegalStateException("Could not prepare isolated config for HotStuffManagerInvariantTest", exception);
    }
  }
}
