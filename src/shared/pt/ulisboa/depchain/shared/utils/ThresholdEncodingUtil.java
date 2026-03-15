package pt.ulisboa.depchain.shared.utils;

import com.weavechain.curve25519.CompressedEdwardsY;
import com.weavechain.curve25519.EdwardsPoint;
import com.weavechain.curve25519.InvalidEncodingException;
import com.weavechain.curve25519.Scalar;

public final class ThresholdEncodingUtil {
  private ThresholdEncodingUtil() {
  }

  public static EdwardsPoint decodeCommitment(byte[] commitment) throws InvalidEncodingException {
    ValidationUtils.requireNonNull(commitment, "commitment");
    return new CompressedEdwardsY(commitment).decompress();
  }

  public static Scalar decodeScalar(byte[] scalarBytes) {
    ValidationUtils.requireNonNull(scalarBytes, "scalarBytes");
    return Scalar.fromCanonicalBytes(scalarBytes.clone());
  }
}
