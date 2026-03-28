package pt.ulisboa.depchain.server.consensus.hotstuff;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;

import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.proto.ConsensusMessageType;
import pt.ulisboa.depchain.proto.Message;
import pt.ulisboa.depchain.proto.Node;
import pt.ulisboa.depchain.proto.NodeCommand;
import pt.ulisboa.depchain.proto.PhaseCertificateMessage;
import pt.ulisboa.depchain.proto.QuorumCertificate;
import pt.ulisboa.depchain.proto.TransactionBatchNodeCommand;
import pt.ulisboa.depchain.proto.TransactionRequest;
import pt.ulisboa.depchain.proto.TransactionType;
import pt.ulisboa.depchain.server.consensus.threshold.ThresholdSignatureProtocol;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.crypto.ClientRequestSignaturePayloadUtil;
import pt.ulisboa.depchain.shared.crypto.CryptoUtil;
import pt.ulisboa.depchain.shared.crypto.key.PrivateKeyLoader;
import pt.ulisboa.depchain.shared.crypto.key.ThresholdKeyLoader;
import pt.ulisboa.depchain.shared.time.TimeUtil;
import pt.ulisboa.depchain.testsupport.TestKeyMaterialSupport;

class HotStuffInboxTest {
  private static final String TEST_RECIPIENT_ADDRESS = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

  @BeforeAll
  static void ensureKeyMaterial() throws Exception {
    TestKeyMaterialSupport.ensureKeyMaterial(configPath());
  }

  @Test
  void waitForQcMessageUntilSkipsOutOfContextQcAndRestoresItToQueue() throws Exception {
    HotStuffInbox inbox = inbox();
    Node wrongNode = createNode(1, "wrong", 901L);
    Node expectedNode = createNode(1, "expected", 902L);

    inbox.offer(qcMessage(1, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT, 4, wrongNode));
    inbox.offer(qcMessage(1, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT, 4, expectedNode));

    Message accepted = inbox.waitForQcMessageUntil(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT, 4, 1, TimeUtil
        .monotonicDeadlineAfterNow(500), qc -> qc.getCertifiedNode().getNodeHash().equals(expectedNode.getNodeHash()));

    assertEquals(expectedNode.getNodeHash(), accepted.getPhaseCertificate().getJustifyQc().getCertifiedNode().getNodeHash());
    Message deferred = inbox.sharedQueueForThreshold().pollFirst();
    assertEquals(wrongNode.getNodeHash(), deferred.getPhaseCertificate().getJustifyQc().getCertifiedNode().getNodeHash());
  }

  @Test
  void waitForValidNewViewsUntilIgnoresLocalAndInvalidRemoteMessages() throws Exception {
    HotStuffInbox inbox = inbox();
    Node invalidNode = createNode(1, "invalid", 903L);
    Node firstValidNode = createNode(2, "first-valid", 904L);
    Node secondValidNode = createNode(3, "second-valid", 905L);

    inbox.offer(newViewMessage(0, 5, invalidNode));
    inbox.offer(newViewMessage(1, 5, invalidNode));
    inbox.offer(qcMessage(2, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_PRE_COMMIT, 5, invalidNode));
    inbox.offer(newViewMessage(2, 5, firstValidNode));
    inbox.offer(newViewMessage(3, 5, secondValidNode));

    List<Message> quorum = inbox.waitForValidNewViewsUntil(5, 2, TimeUtil
        .monotonicDeadlineAfterNow(500), qc -> qc.getMessageType() == ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_NEW_VIEW && qc.getCertifiedNode().getViewNumber() >= 2);

    assertEquals(List.of(2, 3), quorum.stream().map(Message::getReplicaSenderId).toList());
    Message deferred = inbox.sharedQueueForThreshold().pollFirst();
    assertFalse(deferred.getReplicaSenderId() == 2 || deferred.getReplicaSenderId() == 3);
  }

  @Test
  void waitForQcMessageUntilRejectsWrongSenderAndWrongViewBeforeAcceptingExpectedQc() throws Exception {
    HotStuffInbox inbox = inbox();
    Node wrongViewNode = createNode(4, "wrong-view", 906L);
    Node wrongSenderNode = createNode(5, "wrong-sender", 907L);
    Node expectedNode = createNode(5, "expected", 908L);

    inbox.offer(qcMessage(1, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_COMMIT, 4, wrongViewNode));
    inbox.offer(qcMessage(2, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_COMMIT, 5, wrongSenderNode));
    inbox.offer(qcMessage(1, ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_COMMIT, 5, expectedNode));

    Message accepted = inbox.waitForQcMessageUntil(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_COMMIT, 5, 1, TimeUtil
        .monotonicDeadlineAfterNow(500), qc -> qc.getCertifiedNode().getNodeHash().equals(expectedNode.getNodeHash()));

    assertEquals(expectedNode.getNodeHash(), accepted.getPhaseCertificate().getJustifyQc().getCertifiedNode().getNodeHash());
    Message firstDeferred = inbox.sharedQueueForThreshold().pollFirst();
    Message secondDeferred = inbox.sharedQueueForThreshold().pollFirst();
    assertEquals(wrongViewNode.getNodeHash(), firstDeferred.getPhaseCertificate().getJustifyQc().getCertifiedNode().getNodeHash());
    assertEquals(wrongSenderNode.getNodeHash(), secondDeferred.getPhaseCertificate().getJustifyQc().getCertifiedNode().getNodeHash());
  }

  private static HotStuffInbox inbox() throws Exception {
    ConfigParser config = ConfigParser.load(configPath());
    ThresholdSignatureProtocol protocol = new ThresholdSignatureProtocol(0, config, ThresholdKeyLoader.loadReplicaThresholdPrivateShare(config, 0L),
        ThresholdKeyLoader.loadReplicaThresholdPublicKey(config, 0L));
    return new HotStuffInbox(0, protocol);
  }

  private static Message newViewMessage(int senderId, int viewNumber, Node node) {
    QuorumCertificate qc = QuorumCertificate.newBuilder().setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_NEW_VIEW).setViewNumber(viewNumber).setCertifiedNode(node)
        .build();
    return Message.newBuilder().setReplicaSenderId(senderId).setViewNumber(viewNumber).setMessageType(ConsensusMessageType.CONSENSUS_MESSAGE_TYPE_NEW_VIEW)
        .setPhaseCertificate(PhaseCertificateMessage.newBuilder().setJustifyQc(qc)).build();
  }

  private static Message qcMessage(int senderId, ConsensusMessageType type, int viewNumber, Node node) {
    QuorumCertificate qc = QuorumCertificate.newBuilder().setMessageType(type).setViewNumber(viewNumber).setCertifiedNode(node).build();
    return Message.newBuilder().setReplicaSenderId(senderId).setViewNumber(viewNumber).setMessageType(type)
        .setPhaseCertificate(PhaseCertificateMessage.newBuilder().setJustifyQc(qc)).build();
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
      return TestKeyMaterialSupport.isolatedConfigPath("HotStuffInboxTest");
    } catch (IOException exception) {
      throw new IllegalStateException("Could not prepare isolated config for HotStuffInboxTest", exception);
    }
  }
}
