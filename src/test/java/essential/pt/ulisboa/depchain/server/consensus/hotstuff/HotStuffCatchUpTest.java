package pt.ulisboa.depchain.server.consensus.hotstuff;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;

import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.ConsensusMessageType;
import pt.ulisboa.depchain.proto.FetchNodeResponseMessage;
import pt.ulisboa.depchain.proto.Message;
import pt.ulisboa.depchain.proto.Node;
import pt.ulisboa.depchain.proto.NodeCommand;
import pt.ulisboa.depchain.proto.TransactionBatchNodeCommand;
import pt.ulisboa.depchain.proto.TransactionRequest;
import pt.ulisboa.depchain.proto.TransactionType;
import pt.ulisboa.depchain.server.api.ReplicaClientApi;
import pt.ulisboa.depchain.server.execution.EvmService;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.crypto.ClientRequestSignaturePayloadUtil;
import pt.ulisboa.depchain.shared.crypto.CryptoUtil;
import pt.ulisboa.depchain.shared.crypto.key.PrivateKeyLoader;
import pt.ulisboa.depchain.shared.crypto.key.PublicKeyLoader;
import pt.ulisboa.depchain.shared.crypto.key.ThresholdKeyLoader;
import pt.ulisboa.depchain.shared.time.TimeUtil;
import pt.ulisboa.depchain.testsupport.TestKeyMaterialSupport;

class HotStuffCatchUpTest {
  private static final String TEST_RECIPIENT_ADDRESS = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

  @BeforeAll
  static void ensureKeyMaterial() throws Exception {
    TestKeyMaterialSupport.ensureKeyMaterial(configPath());
  }

  @Test
  void executeCommittedBranchExecutesAllUnexecutedAncestorsAndIsIdempotent() throws Exception {
    HotStuffManager hotStuffManager = createHotStuffManager();
    Node node1 = createNode(1, HotStuffSupport.GENESIS_NODE.getNodeHash(), 101L, "one");
    Node node2 = createNode(2, node1.getNodeHash(), 102L, "two");
    Node node3 = createNode(3, node2.getNodeHash(), 103L, "three");

    putKnownNode(hotStuffManager, node1);
    putKnownNode(hotStuffManager, node2);
    putKnownNode(hotStuffManager, node3);

    invokeEnsureDeliveredBranch(hotStuffManager, node3, -1);
    invokeExecuteCommittedBranch(hotStuffManager, node3);

    Set<String> executedHashes = executedNodeHashes(hotStuffManager);
    Set<ClientRequestKey> completedRequestIds = completedRequestIds(hotStuffManager);
    assertTrue(executedHashes.contains(node1.getNodeHash()));
    assertTrue(executedHashes.contains(node2.getNodeHash()));
    assertTrue(executedHashes.contains(node3.getNodeHash()));
    assertTrue(completedRequestIds.contains(requestKey(101L)));
    assertTrue(completedRequestIds.contains(requestKey(102L)));
    assertTrue(completedRequestIds.contains(requestKey(103L)));
    assertEquals(4, executedHashes.size());
    assertEquals(3, completedRequestIds.size());

    invokeEnsureDeliveredBranch(hotStuffManager, node3, -1);
    invokeExecuteCommittedBranch(hotStuffManager, node3);

    assertEquals(4, executedNodeHashes(hotStuffManager).size());
    assertEquals(3, completedRequestIds(hotStuffManager).size());
  }

  @Test
  void fetchNodeFromReplicasRejectsInvalidResponseAndFallsBackToAlternateReplica() throws Exception {
    HotStuffManager hotStuffManager = createHotStuffManager();
    Node parent = createNode(1, HotStuffSupport.GENESIS_NODE.getNodeHash(), 201L, "parent");
    ClientRequest tamperedRequest = parent.getCommand().getTransactionBatch().getClientRequests(0).toBuilder()
        .setTransaction(parent.getCommand().getTransactionBatch().getClientRequests(0).getTransaction().toBuilder().setAmount(999L)).build();
    Node invalidParent = parent.toBuilder()
        .setCommand(parent.getCommand().toBuilder().setTransactionBatch(parent.getCommand().getTransactionBatch().toBuilder().setClientRequests(0, tamperedRequest))).build();

    hotStuffManager.onReplicaMessage(fetchNodeResponse(1, invalidParent));
    hotStuffManager.onReplicaMessage(fetchNodeResponse(2, parent));

    Node fetched = invokeFetchNodeFromReplicas(hotStuffManager, 1, parent.getNodeHash(), TimeUtil.monotonicDeadlineAfterNow(1000));

    assertSame(parent, blockTree(hotStuffManager).get(parent.getNodeHash()));
    assertEquals(parent.getNodeHash(), fetched.getNodeHash());
  }

  @Test
  void fetchNodeFromReplicasIgnoresResponseForWrongHashAndUsesMatchingResponse() throws Exception {
    HotStuffManager hotStuffManager = createHotStuffManager();
    Node requestedParent = createNode(1, HotStuffSupport.GENESIS_NODE.getNodeHash(), 211L, "requested-parent");
    Node wrongHashNode = createNode(1, HotStuffSupport.GENESIS_NODE.getNodeHash(), 212L, "wrong-hash-parent");

    hotStuffManager.onReplicaMessage(fetchNodeResponse(1, wrongHashNode));
    hotStuffManager.onReplicaMessage(fetchNodeResponse(2, requestedParent));

    Node fetched = invokeFetchNodeFromReplicas(hotStuffManager, 1, requestedParent.getNodeHash(), TimeUtil.monotonicDeadlineAfterNow(1000));

    assertEquals(requestedParent.getNodeHash(), fetched.getNodeHash());
    assertSame(requestedParent, blockTree(hotStuffManager).get(requestedParent.getNodeHash()));
  }

  @Test
  void executeCommittedBranchFetchesMissingAncestorsBeforeExecution() throws Exception {
    HotStuffManager hotStuffManager = createHotStuffManager();
    Node parent = createNode(1, HotStuffSupport.GENESIS_NODE.getNodeHash(), 301L, "parent");
    Node child = createNode(2, parent.getNodeHash(), 302L, "child");
    putKnownNode(hotStuffManager, child);

    Thread.ofVirtual().start(() -> {
      await().pollDelay(Duration.ofMillis(50)).forever().untilAsserted(() -> hotStuffManager.onReplicaMessage(fetchNodeResponse(1, parent)));
    });

    invokeEnsureDeliveredBranch(hotStuffManager, child, 1);
    invokeExecuteCommittedBranch(hotStuffManager, child);

    assertTrue(executedNodeHashes(hotStuffManager).contains(parent.getNodeHash()));
    assertTrue(executedNodeHashes(hotStuffManager).contains(child.getNodeHash()));
    assertTrue(completedRequestIds(hotStuffManager).contains(requestKey(301L)));
    assertTrue(completedRequestIds(hotStuffManager).contains(requestKey(302L)));
    assertSame(parent, blockTree(hotStuffManager).get(parent.getNodeHash()));
  }

  @Test
  void ensureDeliveredBranchTimesOutWhenFetchedResponsesNeverMatchRequestedAncestor() throws Exception {
    HotStuffManager hotStuffManager = createHotStuffManager();
    Node realParent = createNode(1, HotStuffSupport.GENESIS_NODE.getNodeHash(), 311L, "real-parent");
    Node child = createNode(2, realParent.getNodeHash(), 312L, "child");
    Node wrongHashParent = createNode(1, HotStuffSupport.GENESIS_NODE.getNodeHash(), 313L, "wrong-hash-parent");
    putKnownNode(hotStuffManager, child);

    Thread.ofVirtual().start(() -> {
      await().pollDelay(Duration.ofMillis(50)).forever().untilAsserted(() -> hotStuffManager.onReplicaMessage(fetchNodeResponse(1, wrongHashParent)));
    });

    assertThrows(InvocationTargetException.class, () -> invokeEnsureDeliveredBranch(hotStuffManager, child, 1));
    assertTrue(blockTree(hotStuffManager).containsKey(child.getNodeHash()));
    assertTrue(!blockTree(hotStuffManager).containsKey(wrongHashParent.getNodeHash()));
  }

  @Test
  void executeCommittedBranchInvokesExecutionHookForEachUnexecutedNode() throws Exception {
    List<String> executedByHook = new ArrayList<>();
    HotStuffManager hotStuffManager = createHotStuffManager(node -> executedByHook.add(node.getNodeHash()));
    Node node1 = createNode(1, HotStuffSupport.GENESIS_NODE.getNodeHash(), 401L, "one");
    Node node2 = createNode(2, node1.getNodeHash(), 402L, "two");
    Node node3 = createNode(3, node2.getNodeHash(), 403L, "three");

    putKnownNode(hotStuffManager, node1);
    putKnownNode(hotStuffManager, node2);
    putKnownNode(hotStuffManager, node3);

    invokeEnsureDeliveredBranch(hotStuffManager, node3, -1);
    invokeExecuteCommittedBranch(hotStuffManager, node3);

    assertEquals(List.of(node1.getNodeHash(), node2.getNodeHash(), node3.getNodeHash()), executedByHook);
  }

  private static HotStuffManager createHotStuffManager() throws Exception {
    return createHotStuffManager(node -> {
    });
  }

  private static HotStuffManager createHotStuffManager(Consumer<Node> onNodeExecuted) throws Exception {
    ConfigParser config = ConfigParser.load(configPath());
    PublicKey clientPublicKey = PublicKeyLoader.loadClientPublicKey(config);
    return new HotStuffManager(0, config, ThresholdKeyLoader.loadReplicaThresholdPrivateShare(config, 0L), ThresholdKeyLoader.loadReplicaThresholdPublicKey(config, 0L),
        Map.of(config.client().senderId(), clientPublicKey), new EvmService(), null, onNodeExecuted);
  }

  private static Node createNode(int viewNumber, String parentHash, long requestId, String value) {
    NodeCommand command = NodeCommand.newBuilder()
        .setTransactionBatch(TransactionBatchNodeCommand.newBuilder().addClientRequests(signedTransferRequest(requestId, value, viewNumber - 1L))).build();
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
      return ClientRequest.newBuilder().setTransaction(TransactionRequest.newBuilder().setRequestKey(requestKey(requestId)).setType(TransactionType.TRANSACTION_TYPE_TRANSFER)
          .setTo(TEST_RECIPIENT_ADDRESS).setAmount(amount).setNonce(nonce).setGasLimit(21_000L).setGasPrice(1L).setSignature(ByteString.copyFrom(signature))).build();
    } catch (Exception exception) {
      throw new IllegalStateException("Could not create signed transfer request", exception);
    }
  }

  private static ClientRequestKey requestKey(long requestId) {
    try {
      ConfigParser config = ConfigParser.load(configPath());
      return ClientRequestKey.newBuilder().setClientSenderId(config.client().senderId()).setRequestId(requestId).build();
    } catch (Exception exception) {
      throw new IllegalStateException("Could not resolve client sender id for request key", exception);
    }
  }

  private static Message fetchNodeResponse(int senderId, Node node) {
    return Message.newBuilder().setViewNumber(Math.max(0, node.getViewNumber())).setReplicaSenderId(senderId)
        .setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_FETCH_NODE_RESPONSE).setFetchNodeResponse(FetchNodeResponseMessage.newBuilder().setNode(node)).build();
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Node> blockTree(HotStuffManager hotStuffManager) throws Exception {
    return (Map<String, Node>) getField(hotStuffManager, "blockTree");
  }

  @SuppressWarnings("unchecked")
  private static Set<String> executedNodeHashes(HotStuffManager hotStuffManager) throws Exception {
    return (Set<String>) getField(hotStuffManager, "executedNodeHashes");
  }

  @SuppressWarnings("unchecked")
  private static Set<ClientRequestKey> completedRequestIds(HotStuffManager hotStuffManager) throws Exception {
    Object clientApi = getField(hotStuffManager, "clientApi");
    Field field = ReplicaClientApi.class.getDeclaredField("completedRequestIds");
    field.setAccessible(true);
    return (Set<ClientRequestKey>) field.get(clientApi);
  }

  private static void putKnownNode(HotStuffManager hotStuffManager, Node node) throws Exception {
    Map<String, Node> blockTree = new HashMap<>(blockTree(hotStuffManager));
    blockTree.put(node.getNodeHash(), node);
    setField(hotStuffManager, "blockTree", blockTree);
  }

  private static Node invokeFetchNodeFromReplicas(HotStuffManager hotStuffManager, int sourceSenderId, String nodeHash, long deadlineNanos) throws Exception {
    Method method = HotStuffManager.class.getDeclaredMethod("fetchNodeFromReplicas", int.class, String.class, long.class);
    method.setAccessible(true);
    return (Node) method.invoke(hotStuffManager, sourceSenderId, nodeHash, deadlineNanos);
  }

  private static void invokeEnsureDeliveredBranch(HotStuffManager hotStuffManager, Node node, int sourceSenderId) throws Exception {
    Method method = HotStuffManager.class.getDeclaredMethod("ensureDeliveredBranch", Node.class, int.class, long.class);
    method.setAccessible(true);
    method.invoke(hotStuffManager, node, sourceSenderId, TimeUtil.monotonicDeadlineAfterNow(1000));
  }

  private static void invokeExecuteCommittedBranch(HotStuffManager hotStuffManager, Node node) throws Exception {
    Method method = HotStuffManager.class.getDeclaredMethod("executeCommittedBranch", Node.class);
    method.setAccessible(true);
    method.invoke(hotStuffManager, node);
  }

  private static Object getField(HotStuffManager hotStuffManager, String fieldName) throws Exception {
    Field field = HotStuffManager.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.get(hotStuffManager);
  }

  private static void setField(HotStuffManager hotStuffManager, String fieldName, Object value) throws Exception {
    Field field = HotStuffManager.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(hotStuffManager, value);
  }

  private static Path configPath() {
    try {
      return TestKeyMaterialSupport.isolatedConfigPath("HotStuffCatchUpTest");
    } catch (IOException exception) {
      throw new IllegalStateException("Could not prepare isolated config for HotStuffCatchUpTest", exception);
    }
  }
}
