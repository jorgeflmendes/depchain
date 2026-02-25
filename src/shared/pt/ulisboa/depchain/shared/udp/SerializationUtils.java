package pt.ulisboa.depchain.shared.udp;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public final class SerializationUtils {
  private SerializationUtils() {
  }

  // utility methods to serialize/deserialize objects to/from byte arrays for sending over UDP
  public static byte[] toBytes(Object value) throws IOException {
    try (ByteArrayOutputStream output = new ByteArrayOutputStream();
         ObjectOutputStream objectOutput = new ObjectOutputStream(output)) {
      objectOutput.writeObject(value);
      objectOutput.flush();
      return output.toByteArray();
    }
  }

  // deserialize an object from a byte array with given offset and length (used when receiving UDP packets that may contain multiple messages)
  public static Object fromBytes(byte[] bytes, int offset, int length) throws IOException, ClassNotFoundException {
    try (ByteArrayInputStream input = new ByteArrayInputStream(bytes, offset, length); ObjectInputStream objectInput = new ObjectInputStream(input)) {
      return objectInput.readObject();
    }
  }
}
