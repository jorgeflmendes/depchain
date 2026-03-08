package pt.ulisboa.depchain.shared.utils;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public final class KeyUtil {
  private KeyUtil() {
  }

  public static String encodePem(String type, byte[] encodedBytes) {
    ValidationUtils.requireAllNonNull(named("type", type), named("encodedBytes", encodedBytes));

    String base64 = Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(encodedBytes);
    return "-----BEGIN " + type + "-----\n" + base64 + "\n-----END " + type + "-----\n";
  }

  public static byte[] decodePemIfNeeded(byte[] fileBytes, String beginMarker, String endMarker) {
    ValidationUtils.requireAllNonNull(named("fileBytes", fileBytes), named("beginMarker", beginMarker), named("endMarker", endMarker));

    String fileText = new String(fileBytes, StandardCharsets.UTF_8);
    if (!fileText.contains(beginMarker)) {
      return fileBytes;
    }

    String pemBody = fileText.replace(beginMarker, "").replace(endMarker, "").replaceAll("\\s+", "");
    return Base64.getDecoder().decode(pemBody);
  }
}
