package pt.ulisboa.depchain.shared.utils;

import java.util.Collection;
import java.util.Objects;

import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.Nullable;

public final class ValidationUtils {
  private ValidationUtils() {
  }

  // To hold a named argument for validation purposes.
  public record NamedArg(String name, Object value) {
    public NamedArg {
      name = requireNonNull(name, "name");
      if (name.isBlank()) {
        throw new IllegalArgumentException("name must not be blank");
      }
    }
  }

  // Create a NamedArg instance for the given name and value.
  public static NamedArg named(String name, Object value) {
    return new NamedArg(name, value);
  }

  // Validate that the given value is not null, throwing a NullPointerException with the specified
  // message if it is.
  public static <T extends Object> @NonNull T requireNonNull(@Nullable T value, String fieldName) {
    return Objects.requireNonNull(value, fieldName + " cannot be null");
  }

  // Validate that all the given NamedArg instances have non-null values, throwing a
  // NullPointerException with the respective field name if any of them is null.
  public static void requireAllNonNull(NamedArg... args) {
    for (NamedArg namedArg : args) {
      requireNonNull(namedArg.value(), namedArg.name());
    }
  }

  // Validate that the given value is not null, throwing an IllegalArgumentException with the
  // specified message if it is.
  public static <T extends Object> @NonNull T requirePresent(@Nullable T value, String message) {
    if (value == null) {
      throw new IllegalArgumentException(message);
    }

    return value;
  }

  // Validate that the given string is not blank.
  public static @NonNull String requireNonBlank(@Nullable String value, String fieldName) {
    String nonNullValue = requireNonNull(value, fieldName);

    if (nonNullValue.isBlank()) {
      throw new IllegalArgumentException("%s must not be blank".formatted(fieldName));
    }

    return nonNullValue;
  }

  // Validate that all the given values are not null, throwing an IllegalArgumentException with the
  // specified message if any of them is null.
  public static void requireAllPresent(String message, Object... values) {
    for (Object value : values) {
      if (value == null) {
        throw new IllegalArgumentException(message);
      }
    }
  }

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

  // Validate that the given int value is at least the specified minimum (inclusive).
  public static int requireAtLeastInt(int value, int minimum, String fieldName, String minimumLabel) {
    if (value < minimum) {
      throw new IllegalArgumentException("%s must be >= %s".formatted(fieldName, minimumLabel));
    }

    return value;
  }

  // Validate that the given value is in the closed range [minimumInclusive, maximumInclusive].
  public static int requireInClosedRangeInt(int value, int minimumInclusive, int maximumInclusive, String fieldName) {
    if (value < minimumInclusive || value > maximumInclusive) {
      throw new IllegalArgumentException("%s must be in range [%d, %d]".formatted(fieldName, minimumInclusive, maximumInclusive));
    }

    return value;
  }

  // Validate that the given value is in the half-open range [minimumInclusive, maximumExclusive).
  public static double requireInHalfOpenRangeDouble(double value, double minimumInclusive, double maximumExclusive, String fieldName) {
    if (value < minimumInclusive || value >= maximumExclusive) {
      throw new IllegalArgumentException("%s must be in range [%s, %s)".formatted(fieldName, minimumInclusive, maximumExclusive));
    }

    return value;
  }

  // Validate that the given value is at most the specified maximum (inclusive).
  public static int requireAtMostInt(int value, int maximumInclusive, String fieldName) {
    if (value > maximumInclusive) {
      throw new IllegalArgumentException("%s must be <= %d".formatted(fieldName, maximumInclusive));
    }

    return value;
  }

  // Validate that the given int value matches the expected value exactly.
  public static int requireExactInt(int value, int expectedValue, String fieldName) {
    if (value != expectedValue) {
      throw new IllegalArgumentException("%s must be %d".formatted(fieldName, expectedValue));
    }

    return value;
  }

  // Validate that the given collection is not empty.
  public static <T extends Collection<?>> @NonNull T requireNonEmpty(@Nullable T value, String fieldName) {
    T nonNullValue = requireNonNull(value, fieldName);

    if (nonNullValue.isEmpty()) {
      throw new IllegalArgumentException("%s must not be empty".formatted(fieldName));
    }

    return nonNullValue;
  }

  // Validate that the given byte array slice is within bounds.
  public static void requireValidSlice(byte[] bytes, int offset, int length) {
    byte[] nonNullBytes = requireNonNull(bytes, "bytes");

    if (offset < 0 || length < 0 || offset > nonNullBytes.length || (offset + length) > nonNullBytes.length) {
      throw new IllegalArgumentException("Invalid byte array slice");
    }
  }
}
