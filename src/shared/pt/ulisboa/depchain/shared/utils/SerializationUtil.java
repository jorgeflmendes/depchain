package pt.ulisboa.depchain.shared.utils;

import java.nio.charset.StandardCharsets;

public final class SerializationUtil {
  private SerializationUtil() {}

  /**
   * Encodes a string value to UTF-8 bytes.
   * Used for both client requests and network payloads.
   */
  public static byte[] encodeString(String value) {
    ValidationUtils.requireNonNull(value, "value");
    return value.getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Decodes UTF-8 bytes to a string value.
   * Used for both client replies and network payloads.
   */
  public static String decodeString(byte[] payload) {
    ValidationUtils.requireNonNull(payload, "payload");
    return new String(payload, StandardCharsets.UTF_8);
  }
}
