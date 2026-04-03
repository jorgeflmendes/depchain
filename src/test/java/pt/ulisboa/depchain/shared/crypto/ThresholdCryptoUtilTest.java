package pt.ulisboa.depchain.shared.crypto;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

class ThresholdCryptoUtilTest {

  @Test
  void partialSignContextRejectsReplicaIndexOutsideParticipantSet() throws Exception {
    ThresholdCryptoUtil.ThresholdConfig config = ThresholdCryptoUtil.createThresholdConfig(4, 3);
    byte[] payload = "threshold-context".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    ThresholdCryptoUtil.ThresholdNonceShare nonceShare = ThresholdCryptoUtil.thresholdNonceShare(payload, config.privateShare(0));

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> new ThresholdCryptoUtil.ThresholdPartialSignContext(0, 4, 3, Set.of(1, 2, 3),
        config.publicKey(), nonceShare.commitment()));
    assertTrue(exception.getMessage().contains("participantIndexes must include replicaIndex"));
  }

  @Test
  void aggregateCommitmentsRejectsWrongCardinality() throws Exception {
    ThresholdCryptoUtil.ThresholdConfig config = ThresholdCryptoUtil.createThresholdConfig(4, 3);
    byte[] payload = "threshold-cardinality".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    ThresholdCryptoUtil.ThresholdNonceShare nonce0 = ThresholdCryptoUtil.thresholdNonceShare(payload, config.privateShare(0));
    ThresholdCryptoUtil.ThresholdNonceShare nonce1 = ThresholdCryptoUtil.thresholdNonceShare(payload, config.privateShare(1));

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> ThresholdCryptoUtil
        .thresholdAggregateCommitments(4, 3, List.of(nonce0.commitment(), nonce1.commitment())));
    assertTrue(exception.getMessage().contains("commitments.size"));
  }

  @Test
  void combinePartialSignaturesRejectsWrongCardinality() throws Exception {
    ThresholdCryptoUtil.ThresholdConfig config = ThresholdCryptoUtil.createThresholdConfig(4, 3);
    byte[] payload = "threshold-signature-count".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    ThresholdCryptoUtil.ThresholdNonceShare nonce0 = ThresholdCryptoUtil.thresholdNonceShare(payload, config.privateShare(0));
    ThresholdCryptoUtil.ThresholdNonceShare nonce1 = ThresholdCryptoUtil.thresholdNonceShare(payload, config.privateShare(1));
    ThresholdCryptoUtil.ThresholdNonceShare nonce2 = ThresholdCryptoUtil.thresholdNonceShare(payload, config.privateShare(2));
    byte[] aggregatedCommitment = ThresholdCryptoUtil.thresholdAggregateCommitments(4, 3, List.of(nonce0.commitment(), nonce1.commitment(), nonce2.commitment()));
    Set<Integer> participants = Set.of(0, 1, 2);

    byte[] sig0 = ThresholdCryptoUtil.thresholdPartialSign(payload, config.privateShare(0), nonce0, new ThresholdCryptoUtil.ThresholdPartialSignContext(0, 4, 3, participants,
        config.publicKey(), aggregatedCommitment));
    byte[] sig1 = ThresholdCryptoUtil.thresholdPartialSign(payload, config.privateShare(1), nonce1, new ThresholdCryptoUtil.ThresholdPartialSignContext(1, 4, 3, participants,
        config.publicKey(), aggregatedCommitment));

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> ThresholdCryptoUtil
        .thresholdCombinePartialSignatures(4, 3, aggregatedCommitment, List.of(sig0, sig1)));
    assertTrue(exception.getMessage().contains("partialSignatures.size"));
  }

  @Test
  void verifyThresholdSignatureRejectsTamperedPayload() throws Exception {
    ThresholdCryptoUtil.ThresholdConfig config = ThresholdCryptoUtil.createThresholdConfig(4, 3);
    byte[] payload = "threshold-valid-signature".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    ThresholdCryptoUtil.ThresholdNonceShare nonce0 = ThresholdCryptoUtil.thresholdNonceShare(payload, config.privateShare(0));
    ThresholdCryptoUtil.ThresholdNonceShare nonce1 = ThresholdCryptoUtil.thresholdNonceShare(payload, config.privateShare(1));
    ThresholdCryptoUtil.ThresholdNonceShare nonce2 = ThresholdCryptoUtil.thresholdNonceShare(payload, config.privateShare(2));
    byte[] aggregatedCommitment = ThresholdCryptoUtil.thresholdAggregateCommitments(4, 3, List.of(nonce0.commitment(), nonce1.commitment(), nonce2.commitment()));
    Set<Integer> participants = Set.of(0, 1, 2);

    byte[] sig0 = ThresholdCryptoUtil.thresholdPartialSign(payload, config.privateShare(0), nonce0, new ThresholdCryptoUtil.ThresholdPartialSignContext(0, 4, 3, participants,
        config.publicKey(), aggregatedCommitment));
    byte[] sig1 = ThresholdCryptoUtil.thresholdPartialSign(payload, config.privateShare(1), nonce1, new ThresholdCryptoUtil.ThresholdPartialSignContext(1, 4, 3, participants,
        config.publicKey(), aggregatedCommitment));
    byte[] sig2 = ThresholdCryptoUtil.thresholdPartialSign(payload, config.privateShare(2), nonce2, new ThresholdCryptoUtil.ThresholdPartialSignContext(2, 4, 3, participants,
        config.publicKey(), aggregatedCommitment));
    byte[] signature = ThresholdCryptoUtil.thresholdCombinePartialSignatures(4, 3, aggregatedCommitment, List.of(sig0, sig1, sig2));

    assertTrue(ThresholdCryptoUtil.verifyThresholdSignature(payload, signature, config.publicKey()));
    assertFalse(ThresholdCryptoUtil.verifyThresholdSignature("threshold-tampered-signature".getBytes(java.nio.charset.StandardCharsets.UTF_8), signature, config.publicKey()));
  }
}
