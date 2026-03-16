package pt.ulisboa.depchain.shared.utils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;

import com.weavechain.curve25519.EdwardsPoint;
import com.weavechain.curve25519.Scalar;
import com.weavechain.sig.ThresholdSigEd25519;
import com.weavechain.sig.ThresholdSigEd25519Params;

public final class ThresholdCryptoUtil {
  private static final HexFormat HEX_FORMAT = HexFormat.of();

  private ThresholdCryptoUtil() {
  }

  public record ThresholdConfig(ThresholdSigEd25519Params params) {
    public ThresholdConfig {
      ValidationUtils.requireNonNull(params, "params");
    }

    public byte[] publicKey() {
      return params.getPublicKey();
    }

    public Scalar privateShare(int replicaIndex) {
      if (replicaIndex < 0 || replicaIndex >= params.getPrivateShares().size()) {
        throw new IllegalArgumentException("Invalid replica index for threshold share");
      }
      return params.getPrivateShares().get(replicaIndex);
    }
  }

  public record ThresholdNonceShare(Scalar nonceShare, byte[] commitment) {
    public ThresholdNonceShare {
      ValidationUtils.requireAllNonNull(ValidationUtils.named("nonceShare", nonceShare), ValidationUtils.named("commitment", commitment));
      ValidationUtils.requirePositiveInt(commitment.length, "commitment.length");
    }
  }

  public record ThresholdPartialSignContext(int replicaIndex, int totalReplicas, int threshold, Set<Integer> participantIndexes, byte[] thresholdPublicKey,
      byte[] aggregatedCommitment) {
    public ThresholdPartialSignContext {
      ValidationUtils.requirePositiveInt(totalReplicas, "totalReplicas");
      ValidationUtils.requirePositiveInt(threshold, "threshold");
      ValidationUtils.requireAtLeastInt(totalReplicas, threshold, "totalReplicas", "threshold");
      ValidationUtils.requireAllNonNull(ValidationUtils.named("participantIndexes", participantIndexes), ValidationUtils
          .named("thresholdPublicKey", thresholdPublicKey), ValidationUtils.named("aggregatedCommitment", aggregatedCommitment));
      ValidationUtils.requireExactInt(participantIndexes.size(), threshold, "participantIndexes.size");
      ValidationUtils.requireInClosedRangeInt(replicaIndex, 0, totalReplicas - 1, "replicaIndex");

      if (!participantIndexes.contains(replicaIndex)) {
        throw new IllegalArgumentException("participantIndexes must include replicaIndex");
      }

      for (Integer participantIndex : participantIndexes) {
        ValidationUtils.requireNonNull(participantIndex, "participantIndex");
        ValidationUtils.requireInClosedRangeInt(participantIndex, 0, totalReplicas - 1, "participantIndex");
      }
    }
  }

  public static ThresholdConfig newThresholdConfig(int totalReplicas, int threshold)
      throws InvalidKeyException, NoSuchAlgorithmException, SignatureException, InvalidKeySpecException, NoSuchProviderException, IOException {
    ThresholdSigEd25519 signatureScheme = new ThresholdSigEd25519(threshold, totalReplicas);
    ThresholdSigEd25519Params params = signatureScheme.generate();
    return new ThresholdConfig(params);
  }

  public static ThresholdNonceShare thresholdNonceShare(byte[] payload, Scalar privateShare) throws Exception {
    ValidationUtils.requireAllNonNull(ValidationUtils.named("payload", payload), ValidationUtils.named("privateShare", privateShare));

    ThresholdSigEd25519 signatureScheme = new ThresholdSigEd25519(1, 1);
    Scalar nonceShare = signatureScheme.computeRi(privateShare, thresholdPayload(payload));
    byte[] commitment = ThresholdSigEd25519.mulBasepoint(nonceShare).compress().toByteArray();
    return new ThresholdNonceShare(nonceShare, commitment);
  }

  public static byte[] thresholdAggregateCommitments(int totalReplicas, int threshold, List<byte[]> commitments) throws Exception {
    ValidationUtils.requirePositiveInt(totalReplicas, "totalReplicas");
    ValidationUtils.requirePositiveInt(threshold, "threshold");
    ValidationUtils.requireAtLeastInt(totalReplicas, threshold, "totalReplicas", "threshold");
    ValidationUtils.requireNonEmpty(commitments, "commitments");
    ValidationUtils.requireExactInt(commitments.size(), threshold, "commitments.size");

    ThresholdSigEd25519 signatureScheme = new ThresholdSigEd25519(threshold, totalReplicas);
    List<EdwardsPoint> commitmentPoints = new ArrayList<>(commitments.size());
    for (byte[] commitment : commitments) {
      commitmentPoints.add(ThresholdEncodingUtil.decodeCommitment(commitment));
    }

    return signatureScheme.computeR(commitmentPoints).compress().toByteArray();
  }

  public static byte[] thresholdPartialSign(byte[] payload, Scalar privateShare, ThresholdNonceShare nonceShare, ThresholdPartialSignContext context) throws Exception {
    ValidationUtils.requireAllNonNull(ValidationUtils.named("payload", payload), ValidationUtils.named("privateShare", privateShare), ValidationUtils
        .named("nonceShare", nonceShare), ValidationUtils.named("context", context));

    ThresholdSigEd25519 signatureScheme = new ThresholdSigEd25519(context.threshold(), context.totalReplicas());
    EdwardsPoint aggregatedCommitment = ThresholdEncodingUtil.decodeCommitment(context.aggregatedCommitment());
    Scalar challenge = signatureScheme.computeK(context.thresholdPublicKey(), aggregatedCommitment, thresholdPayload(payload));
    Scalar partialSignature = signatureScheme.computeSignature(context.replicaIndex() + 1, privateShare, nonceShare.nonceShare(), challenge, context.participantIndexes());

    return partialSignature.toByteArray();
  }

  public static byte[] thresholdCombinePartialSignatures(int totalReplicas, int threshold, byte[] aggregatedCommitment, List<byte[]> partialSignatures) throws Exception {
    ValidationUtils.requirePositiveInt(totalReplicas, "totalReplicas");
    ValidationUtils.requirePositiveInt(threshold, "threshold");
    ValidationUtils.requireAtLeastInt(totalReplicas, threshold, "totalReplicas", "threshold");
    ValidationUtils.requireNonEmpty(partialSignatures, "partialSignatures");
    ValidationUtils.requireExactInt(partialSignatures.size(), threshold, "partialSignatures.size");

    ThresholdSigEd25519 signatureScheme = new ThresholdSigEd25519(threshold, totalReplicas);
    EdwardsPoint aggregatedPoint = ThresholdEncodingUtil.decodeCommitment(aggregatedCommitment);
    List<Scalar> signatureShares = new ArrayList<>(partialSignatures.size());
    for (byte[] partialSignature : partialSignatures) {
      signatureShares.add(ThresholdEncodingUtil.decodeScalar(partialSignature));
    }

    return signatureScheme.computeSignature(aggregatedPoint, signatureShares);
  }

  public static boolean verifyThresholdSignature(byte[] payload, byte[] signature, byte[] thresholdPublicKey) throws Exception {
    ValidationUtils.requireAllNonNull(ValidationUtils.named("payload", payload), ValidationUtils.named("signature", signature), ValidationUtils
        .named("thresholdPublicKey", thresholdPublicKey));

    return ThresholdSigEd25519.verify(thresholdPublicKey, signature, thresholdPayload(payload).getBytes(StandardCharsets.UTF_8));
  }

  private static String thresholdPayload(byte[] payload) {
    ValidationUtils.requireNonNull(payload, "payload");
    return HEX_FORMAT.formatHex(payload);
  }
}
