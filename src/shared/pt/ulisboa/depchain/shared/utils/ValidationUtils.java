package pt.ulisboa.depchain.shared.utils;

import java.util.Objects;

// Utility class for validating various conditions and parameters.
public final class ValidationUtils {
  private ValidationUtils() {}

  // Validate that the given port number is in the valid range [1, 65535].
  public static int requireValidPort(int port, String fieldName) {
    if (port < 1 || port > 65535) {
      throw new IllegalArgumentException("%s must be in range [1, 65535]".formatted(fieldName));
    }
    
    return port;
  }

  // Validate that the given value is a non-negative integer (>= 0).
  public static int requireNonNegativeInt(int value, String fieldName) {
    if (value < 0) {
      throw new IllegalArgumentException("%s must be >= 0".formatted(fieldName));
    }

    return value;
  }

  // Validate that the given value is a non-negative long (>= 0).
  public static long requireNonNegativeLong(long value, String fieldName) {
    if (value < 0) {
      throw new IllegalArgumentException("%s must be >= 0".formatted(fieldName));
    }

    return value;
  }

  // Validate that the given value is a positive integer (> 0).
  public static int requirePositiveInt(int value, String fieldName) {
    if (value <= 0) {
      throw new IllegalArgumentException("%s must be > 0".formatted(fieldName));
    }

    return value;
  }

  // Validate that the given value is a positive long (> 0).
  public static long requirePositiveLong(long value, String fieldName) {
    if (value <= 0L) {
      throw new IllegalArgumentException("%s must be > 0".formatted(fieldName));
    }

    return value;
  }

  // Validate that the given value is at least the specified minimum (inclusive).
  public static long requireAtLeast(long value, long minimum, String fieldName, String minimumLabel) {
    if (value < minimum) {
      throw new IllegalArgumentException("%s must be >= %s".formatted(fieldName, minimumLabel));
    }

    return value;
  }

  // Validate that the given byte array slice is within bounds.
  public static void requireValidSlice(byte[] bytes, int offset, int length) {
    Objects.requireNonNull(bytes, "bytes cannot be null");

    if (offset < 0 || length < 0 || offset > bytes.length || (offset + length) > bytes.length) {
      throw new IllegalArgumentException("Invalid byte array slice");
    }
  }
}
