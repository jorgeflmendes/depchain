package pt.ulisboa.depchain.shared.utils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import pt.ulisboa.depchain.server.Node;
import pt.ulisboa.depchain.server.Message;
import pt.ulisboa.depchain.server.QuorumCertificate;

public final class SerializationUtil {
  private static final byte FLAG_NULL = 0;
  private static final byte FLAG_PRESENT = 1;

  public static byte[] encodeString(String value) {
    ValidationUtils.requireNonNull(value, "value");
    return value.getBytes(StandardCharsets.UTF_8);
  }

  public static String decodeString(byte[] payload) {
    ValidationUtils.requireNonNull(payload, "payload");
    return new String(payload, StandardCharsets.UTF_8);
  }

  private static byte[] encodeNode(Node node) {
    if (node == null) {
      return new byte[]{FLAG_NULL};
    }

    byte[] parentHashBytes = node.getParentHash().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    byte[] thisHashBytes = node.getThisHash().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    byte[] commandBytes = node.getCommand().getBytes(java.nio.charset.StandardCharsets.UTF_8);

    // Calculate buffer capacity
    int capacity = Byte.BYTES + (4 * Integer.BYTES) + parentHashBytes.length + thisHashBytes.length + commandBytes.length;

    ByteBuffer buffer = ByteBuffer.allocate(capacity);

    // Encode fields in a consistent order
    buffer.put(FLAG_PRESENT);
    buffer.putInt(parentHashBytes.length);
    buffer.put(parentHashBytes);
    buffer.putInt(thisHashBytes.length);
    buffer.put(thisHashBytes);
    buffer.putInt(node.getViewNumber());
    buffer.putInt(commandBytes.length);
    buffer.put(commandBytes);

    return buffer.array();
  }

  private static Node decodeNode(ByteBuffer buffer) {
    ValidationUtils.requireNonNull(buffer, "buffer");

    if (buffer.get() == FLAG_NULL) {
      return null;
    }

    // Decode fields in the same order they were encoded
    int parentHashLen = buffer.getInt();
    byte[] parentHashBytes = new byte[parentHashLen];
    buffer.get(parentHashBytes);
    String parentHash = new String(parentHashBytes, java.nio.charset.StandardCharsets.UTF_8);
    int thisHashLen = buffer.getInt();
    byte[] thisHashBytes = new byte[thisHashLen];
    buffer.get(thisHashBytes);
    String thisHash = new String(thisHashBytes, java.nio.charset.StandardCharsets.UTF_8);
    int viewNumber = buffer.getInt();
    int commandLen = buffer.getInt();
    byte[] commandBytes = new byte[commandLen];
    buffer.get(commandBytes);
    String command = new String(commandBytes, java.nio.charset.StandardCharsets.UTF_8);

    return new Node(parentHash, thisHash, viewNumber, command);
  }

  private static byte[] encodeQuorumCertificate(QuorumCertificate qc) {
    if (qc == null) {
      return new byte[]{FLAG_NULL};
    }

    byte[] nodeBytes = encodeNode(qc.getNode());
    java.util.List<byte[]> signatures = qc.getSignatures();

    // Calculate memory needed for signatures
    int sigsCapacity = Integer.BYTES;
    if (signatures != null) {
      for (byte[] sig : signatures) {
        sigsCapacity += Integer.BYTES + sig.length;
      }
    }

    int capacity = Byte.BYTES + (2 * Integer.BYTES) + nodeBytes.length + sigsCapacity;

    ByteBuffer buffer = ByteBuffer.allocate(capacity);

    // Encode fields in a consistent order
    buffer.put(FLAG_PRESENT);
    buffer.putInt(qc.getType().ordinal());
    buffer.putInt(qc.getViewNumber());
    buffer.put(nodeBytes);

    if (signatures == null) {
      buffer.putInt(0);
    } else {
      buffer.putInt(signatures.size());
      for (byte[] sig : signatures) {
        buffer.putInt(sig.length);
        buffer.put(sig);
      }
    }

    return buffer.array();
  }

  private static QuorumCertificate decodeQuorumCertificate(ByteBuffer buffer) {
    ValidationUtils.requireNonNull(buffer, "buffer");

    if (buffer.get() == FLAG_NULL) {
      return null;
    }

    // Decode fields in the same order they were encoded
    int typeOrdinal = buffer.getInt();
    int viewNumber = buffer.getInt();
    Node node = decodeNode(buffer);
    Message.MessageType type = Message.MessageType.values()[typeOrdinal];

    QuorumCertificate qc = new QuorumCertificate(type, viewNumber, node);
    int sigSize = buffer.getInt();
    for (int i = 0; i < sigSize; i++) {
      int sigLen = buffer.getInt();
      byte[] sig = new byte[sigLen];
      buffer.get(sig);
      qc.addSignature(sig);
    }

    return qc;
  }

  public static Message decodeMessage(byte[] payload) {
    ValidationUtils.requireNonNull(payload, "payload");
    ByteBuffer buffer = ByteBuffer.wrap(payload);

    // Decode message fields in the same order they were encoded
    int currView = buffer.getInt();
    int senderId = buffer.getInt();
    int typeOrdinal = buffer.getInt();
    Node node = decodeNode(buffer);
    QuorumCertificate justify = decodeQuorumCertificate(buffer);

    Message.MessageType type = Message.MessageType.values()[typeOrdinal];
    return new Message(currView, senderId, type, node, justify);
  }

  public static byte[] encodeMessage(Message msg) {
    ValidationUtils.requireNonNull(msg, "msg");

    byte[] nodeBytes = encodeNode(msg.getNode());
    byte[] justifyBytes = encodeQuorumCertificate(msg.getJustify());
    int capacity = (3 * Integer.BYTES) + nodeBytes.length + justifyBytes.length;
    ByteBuffer buffer = ByteBuffer.allocate(capacity);

    // Encode message fields in a consistent order
    buffer.putInt(msg.getCurrView());
    buffer.putInt(msg.getSenderId());
    buffer.putInt(msg.getType().ordinal());
    buffer.put(nodeBytes);
    buffer.put(justifyBytes);

    return buffer.array();
  }
}
