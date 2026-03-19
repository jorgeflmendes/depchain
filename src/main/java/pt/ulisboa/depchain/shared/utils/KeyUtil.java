package pt.ulisboa.depchain.shared.utils;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

import java.io.IOException;
import java.io.StringWriter;

import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;

public final class KeyUtil {
  private KeyUtil() {
  }

  public static String encodePem(String type, byte[] encodedBytes) {
    ValidationUtils.requireAllNonNull(named("type", type), named("encodedBytes", encodedBytes));

    try (StringWriter writer = new StringWriter(); PemWriter pemWriter = new PemWriter(writer)) {
      pemWriter.writeObject(new PemObject(type, encodedBytes));
      pemWriter.flush();
      return writer.toString();
    } catch (IOException exception) {
      throw new IllegalStateException("Failed to encode PEM for type " + type, exception);
    }
  }
}
