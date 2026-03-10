package pt.ulisboa.depchain.shared.utils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import pt.ulisboa.depchain.server.consensus.Message;
import pt.ulisboa.depchain.server.consensus.Message.MessageType;
import pt.ulisboa.depchain.server.consensus.Node;
import pt.ulisboa.depchain.server.consensus.QuorumCertificate;
import pt.ulisboa.depchain.shared.utils.ThresholdCryptoUtil.ThresholdConfig;
import pt.ulisboa.depchain.shared.utils.ThresholdCryptoUtil.ThresholdNonceShare;
import pt.ulisboa.depchain.shared.utils.ThresholdCryptoUtil.ThresholdPartialSignContext;

class ThresholdSignatureTest {
  @Test
  void thresholdSignatureRoundTripVerifiesCombinedSignature() throws Exception {
    int totalReplicas = 4;
    int threshold = 3;
    ThresholdConfig thresholdConfig = ThresholdCryptoUtil.newThresholdConfig(totalReplicas, threshold);
    byte[] payload = "PREPARE|7|node-7".getBytes(StandardCharsets.UTF_8);
    List<Integer> participantIndexes = List.of(0, 1, 2);
    Set<Integer> participantSet = new LinkedHashSet<>(participantIndexes);

    List<ThresholdNonceShare> nonces = new ArrayList<>();
    List<byte[]> commitments = new ArrayList<>();
    for (int participantIndex : participantIndexes) {
      ThresholdNonceShare nonce = ThresholdCryptoUtil.thresholdNonceShare(payload, thresholdConfig.privateShare(participantIndex));
      nonces.add(nonce);
      commitments.add(nonce.commitment());
    }

    byte[] aggregatedCommitment = ThresholdCryptoUtil.thresholdAggregateCommitments(totalReplicas, threshold, commitments);
    List<byte[]> partialSignatures = new ArrayList<>();
    for (int i = 0; i < participantIndexes.size(); i++) {
      int participantIndex = participantIndexes.get(i);
      ThresholdPartialSignContext context = new ThresholdPartialSignContext(participantIndex, totalReplicas, threshold, participantSet, thresholdConfig.publicKey(),
          aggregatedCommitment);
      partialSignatures.add(ThresholdCryptoUtil.thresholdPartialSign(payload, thresholdConfig.privateShare(participantIndex), nonces.get(i), context));
    }

    byte[] signature = ThresholdCryptoUtil.thresholdCombinePartialSignatures(totalReplicas, threshold, aggregatedCommitment, partialSignatures);
    assertTrue(ThresholdCryptoUtil.verifyThresholdSignature(payload, signature, thresholdConfig.publicKey()));
  }

  @Test
  void messageSerializationPreservesThresholdVariants() {
    Node node = new Node("parent", "hash", 9, "append:x");
    QuorumCertificate qc = new QuorumCertificate(MessageType.PREPARE, 9, node);
    qc.setAggregatedSignature(new byte[]{8, 7, 6});

    Message signatureMessage = new Message(9, 2, MessageType.COMMIT, node, qc);
    signatureMessage.setSignature(new byte[]{1, 2, 3});
    Message decodedSignature = SerializationUtil.decodeMessage(SerializationUtil.encodeMessage(signatureMessage));

    assertEquals(signatureMessage.getCurrView(), decodedSignature.getCurrView());
    assertEquals(signatureMessage.getSenderId(), decodedSignature.getSenderId());
    assertEquals(signatureMessage.getType(), decodedSignature.getType());
    assertEquals(signatureMessage.getNode().getThisHash(), decodedSignature.getNode().getThisHash());
    assertArrayEquals(signatureMessage.getSignature(), decodedSignature.getSignature());
    assertArrayEquals(qc.getAggregatedSignature(), decodedSignature.getJustify().getAggregatedSignature());

    Message commitmentMessage = new Message(9, 2, MessageType.COMMIT, node, qc);
    commitmentMessage.setPartialCommitment(new byte[]{4, 5, 6});
    Message decodedCommitment = SerializationUtil.decodeMessage(SerializationUtil.encodeMessage(commitmentMessage));

    assertArrayEquals(commitmentMessage.getPartialCommitment(), decodedCommitment.getPartialCommitment());
    assertNotNull(decodedCommitment.getJustify());

    Message contextMessage = new Message(9, 2, MessageType.COMMIT, node, qc);
    contextMessage.setThresholdContext(new byte[]{7, 8, 9}, new int[]{0, 2, 3});
    Message decodedContext = SerializationUtil.decodeMessage(SerializationUtil.encodeMessage(contextMessage));

    assertArrayEquals(contextMessage.getAggregatedCommitment(), decodedContext.getAggregatedCommitment());
    assertArrayEquals(contextMessage.getParticipantIndexes(), decodedContext.getParticipantIndexes());
  }
}
