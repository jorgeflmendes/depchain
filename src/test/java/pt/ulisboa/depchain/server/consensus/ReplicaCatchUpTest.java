package pt.ulisboa.depchain.server.consensus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;

import pt.ulisboa.depchain.proto.AppendNodeCommand;
import pt.ulisboa.depchain.proto.AppendRequest;
import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.ConsensusMessageType;
import pt.ulisboa.depchain.proto.FetchNodeResponseMessage;
import pt.ulisboa.depchain.proto.Message;
import pt.ulisboa.depchain.proto.Node;
import pt.ulisboa.depchain.proto.NodeCommand;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.keys.PrivateKeyLoader;
import pt.ulisboa.depchain.shared.keys.PublicKeyLoader;
import pt.ulisboa.depchain.shared.keys.ThresholdKeyLoader;
import pt.ulisboa.depchain.shared.utils.ClientRequestPayloadUtil;
import pt.ulisboa.depchain.shared.utils.ConsensusPayloadUtil;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;
import pt.ulisboa.depchain.shared.utils.TimeUtil;

class ReplicaCatchUpTest {
  @Test
  void executeCommittedBranchExecutesAllUnexecutedAncestorsAndIsIdempotent() throws Exception {
    HotStuffManager hotStuffManager = newHotStuffManager();
    Node node1 = newNode(1, ConsensusUtil.GENESIS_NODE.getNodeHash(), 101L, "one");
    Node node2 = newNode(2, node1.getNodeHash(), 102L, "two");
    Node node3 = newNode(3, node2.getNodeHash(), 103L, "three");

    putKnownNode(hotStuffManager, node1);
    putKnownNode(hotStuffManager, node2);
    putKnownNode(hotStuffManager, node3);

    invokeEnsureDeliveredBranchOnDecide(hotStuffManager, node3, -1);
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

    invokeEnsureDeliveredBranchOnDecide(hotStuffManager, node3, -1);
    invokeExecuteCommittedBranch(hotStuffManager, node3);

    assertEquals(4, executedNodeHashes(hotStuffManager).size());
    assertEquals(3, completedRequestIds(hotStuffManager).size());
  }

  @Test
  void fetchNodeFromReplicasRejectsInvalidResponseAndFallsBackToAlternateReplica() throws Exception {
    HotStuffManager hotStuffManager = newHotStuffManager();
    Node parent = newNode(1, ConsensusUtil.GENESIS_NODE.getNodeHash(), 201L, "parent");
    ClientRequest tamperedRequest = parent.getCommand().getAppend().getClientRequest().toBuilder()
        .setAppend(parent.getCommand().getAppend().getClientRequest().getAppend().toBuilder().setValue("tampered")).build();
    Node invalidParent = parent.toBuilder().setCommand(parent.getCommand().toBuilder().setAppend(parent.getCommand().getAppend().toBuilder().setClientRequest(tamperedRequest)))
        .build();

    hotStuffManager.onReplicaMessage(fetchNodeResponse(1, invalidParent));
    hotStuffManager.onReplicaMessage(fetchNodeResponse(2, parent));

    Node fetched = invokeFetchNodeFromReplicas(hotStuffManager, 1, parent.getNodeHash(), TimeUtil.monotonicDeadlineAfterNow(1000));

    assertSame(parent, blockTree(hotStuffManager).get(parent.getNodeHash()));
    assertEquals(parent.getNodeHash(), fetched.getNodeHash());
  }

  @Test
  void executeCommittedBranchFetchesMissingAncestorsBeforeExecution() throws Exception {
    HotStuffManager hotStuffManager = newHotStuffManager();
    Node parent = newNode(1, ConsensusUtil.GENESIS_NODE.getNodeHash(), 301L, "parent");
    Node child = newNode(2, parent.getNodeHash(), 302L, "child");
    putKnownNode(hotStuffManager, child);

    Thread.ofVirtual().start(() -> {
      try {
        Thread.sleep(50);
        hotStuffManager.onReplicaMessage(fetchNodeResponse(1, parent));
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
    });

    invokeEnsureDeliveredBranchOnDecide(hotStuffManager, child, 1);
    invokeExecuteCommittedBranch(hotStuffManager, child);

    assertTrue(executedNodeHashes(hotStuffManager).contains(parent.getNodeHash()));
    assertTrue(executedNodeHashes(hotStuffManager).contains(child.getNodeHash()));
    assertTrue(completedRequestIds(hotStuffManager).contains(requestKey(301L)));
    assertTrue(completedRequestIds(hotStuffManager).contains(requestKey(302L)));
    assertSame(parent, blockTree(hotStuffManager).get(parent.getNodeHash()));
  }

  private static HotStuffManager newHotStuffManager() throws Exception {
    ConfigParser config = ConfigParser.load(configPath());
    PublicKey clientPublicKey = PublicKeyLoader.loadClientPublicKey(config);
    return new HotStuffManager(0, config, ThresholdKeyLoader.loadReplicaThresholdPrivateShare(config, 0L), ThresholdKeyLoader.loadReplicaThresholdPublicKey(config, 0L),
        clientPublicKey);
  }

  private static Node newNode(int viewNumber, String parentHash, long requestId, String value) {
    NodeCommand command = NodeCommand.newBuilder().setAppend(AppendNodeCommand.newBuilder().setClientRequest(signedAppendRequest(requestId, value))).build();
    String nodeHash = CryptoUtil.sha256Hex(ConsensusPayloadUtil.nodeHashPayload(parentHash, viewNumber, command));
    return Node.newBuilder().setParentNodeHash(parentHash).setNodeHash(nodeHash).setViewNumber(viewNumber).setCommand(command).build();
  }

  private static ClientRequest signedAppendRequest(long requestId, String value) {
    try {
      ConfigParser config = ConfigParser.load(configPath());
      long clientSenderId = config.client().senderId();
      PrivateKey clientPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config);
      byte[] signature = CryptoUtil.signEcdsa(ClientRequestPayloadUtil.signedAppendRequestPayload(clientSenderId, requestId, value), clientPrivateKey);
      return ClientRequest.newBuilder().setAppend(AppendRequest.newBuilder().setRequestKey(requestKey(requestId)).setValue(value).setSignature(ByteString.copyFrom(signature)))
          .build();
    } catch (Exception exception) {
      throw new IllegalStateException("Could not create signed append request", exception);
    }
  }

  private static ClientRequestKey requestKey(long requestId) {
    return ClientRequestKey.newBuilder().setClientSenderId(100L).setRequestId(requestId).build();
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
    Object clientCommunication = getField(hotStuffManager, "clientCommunication");
    Field field = ClientCommunicationManager.class.getDeclaredField("completedRequestIds");
    field.setAccessible(true);
    return (Set<ClientRequestKey>) field.get(clientCommunication);
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

  private static void invokeEnsureDeliveredBranchOnDecide(HotStuffManager hotStuffManager, Node node, int sourceSenderId) throws Exception {
    Method method = HotStuffManager.class.getDeclaredMethod("ensureDeliveredBranchOnDecide", Node.class, int.class);
    method.setAccessible(true);
    method.invoke(hotStuffManager, node, sourceSenderId);
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
    return Path.of(System.getProperty("user.dir"), "config", "config.yaml").toAbsolutePath();
  }
}
