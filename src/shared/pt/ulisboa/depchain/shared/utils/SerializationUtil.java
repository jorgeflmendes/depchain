package pt.ulisboa.depchain.shared.utils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.weavechain.curve25519.CompressedEdwardsY;
import com.weavechain.curve25519.EdwardsPoint;
import com.weavechain.curve25519.InvalidEncodingException;
import com.weavechain.curve25519.Scalar;

import pt.ulisboa.depchain.server.consensus.Message;
import pt.ulisboa.depchain.server.consensus.Message.MessageType;
import pt.ulisboa.depchain.server.consensus.Node;
import pt.ulisboa.depchain.server.consensus.QuorumCertificate;

public final class SerializationUtil {
  private static final byte FLAG_NULL = 0;
  private static final byte FLAG_PRESENT = 1;
  private static final int NULL_LENGTH = -1;

  public static byte[] encodeString(String value) {
    ValidationUtils.requireNonNull(value, "value");
    return value.getBytes(StandardCharsets.UTF_8);
  }

  public static String decodeString(byte[] payload) {
    ValidationUtils.requireNonNull(payload, "payload");
    return new String(payload, StandardCharsets.UTF_8);
  }

  public static byte[] encodeVotePayload(MessageType type, int viewNumber, Node node) {
    ValidationUtils.requireNonNull(type, "type");
    ValidationUtils.requireNonNegativeInt(viewNumber, "viewNumber");

    String nodeHash = "null";
    if (node != null) {
      nodeHash = node.getThisHash();
    }

    return encodeString(type.name() + "|" + viewNumber + "|" + nodeHash);
  }

  public static EdwardsPoint decodeCommitment(byte[] commitment) throws InvalidEncodingException {
    ValidationUtils.requireNonNull(commitment, "commitment");
    return new CompressedEdwardsY(commitment).decompress();
  }

  public static Scalar decodeScalar(byte[] scalarBytes) {
    ValidationUtils.requireNonNull(scalarBytes, "scalarBytes");
    return Scalar.fromCanonicalBytes(scalarBytes.clone());
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
    byte[] sig = qc.getAggregatedSignature();

    // Calculate memory needed for signatures
    int sigCapacity = Integer.BYTES;
    if (sig != null) {
      sigCapacity += sig.length;
    }

    int capacity = Byte.BYTES + (2 * Integer.BYTES) + nodeBytes.length + sigCapacity;

    ByteBuffer buffer = ByteBuffer.allocate(capacity);

    // Encode fields in a consistent order
    buffer.put(FLAG_PRESENT);
    buffer.putInt(qc.getType().ordinal());
    buffer.putInt(qc.getViewNumber());
    buffer.put(nodeBytes);

    if (sig == null) {
      buffer.putInt(0);
    } else {
      buffer.putInt(sig.length);
      buffer.put(sig);
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
    int sigLen = buffer.getInt();
    if (sigLen > 0) {
      byte[] sig = new byte[sigLen];
      buffer.get(sig);
      qc.setAggregatedSignature(sig);
    }

    return qc;
  }

  private static int nullableByteArrayCapacity(byte[] value) {
    int capacity = Integer.BYTES;
    if (value != null) {
      capacity += value.length;
    }
    return capacity;
  }

  private static void putNullableByteArray(ByteBuffer buffer, byte[] value) {
    if (value == null) {
      buffer.putInt(NULL_LENGTH);
    } else {
      buffer.putInt(value.length);
      buffer.put(value);
    }
  }

  private static byte[] getNullableByteArray(ByteBuffer buffer) {
    int length = buffer.getInt();
    if (length < 0) {
      return null;
    }

    byte[] bytes = new byte[length];
    buffer.get(bytes);
    return bytes;
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
    byte[] signature = getNullableByteArray(buffer);
    byte[] partialCommitment = getNullableByteArray(buffer);
    byte[] aggregatedCommitment = getNullableByteArray(buffer);

    int participantCount = buffer.getInt();
    int[] participantIndexes = null;
    if (participantCount >= 0) {
      participantIndexes = new int[participantCount];
      for (int i = 0; i < participantCount; i++) {
        participantIndexes[i] = buffer.getInt();
      }
    }

    Message.MessageType type = Message.MessageType.values()[typeOrdinal];
    Message message = new Message(currView, senderId, type, node, justify);
    int populatedVariants = 0;
    if (signature != null) {
      populatedVariants++;
    }
    if (partialCommitment != null) {
      populatedVariants++;
    }
    if (aggregatedCommitment != null || participantIndexes != null) {
      populatedVariants++;
    }

    if (populatedVariants > 1) {
      throw new IllegalArgumentException("Message cannot contain multiple threshold payload variants");
    }
    if (signature != null) {
      message.setSignature(signature);
    } else if (partialCommitment != null) {
      message.setPartialCommitment(partialCommitment);
    } else if (aggregatedCommitment != null || participantIndexes != null) {
      if (aggregatedCommitment == null || participantIndexes == null) {
        throw new IllegalArgumentException("Threshold context requires both aggregatedCommitment and participantIndexes");
      }
      message.setThresholdContext(aggregatedCommitment, participantIndexes);
    }
    return message;
  }

  public static byte[] encodeMessage(Message msg) {
    ValidationUtils.requireNonNull(msg, "msg");

    byte[] nodeBytes = encodeNode(msg.getNode());
    byte[] justifyBytes = encodeQuorumCertificate(msg.getJustify());
    byte[] signature = null;
    byte[] partialCommitment = null;
    byte[] aggregatedCommitment = null;
    int[] participantIndexes = null;
    switch (msg.getThresholdPayloadType()) {
      case SIGNATURE_SHARE :
        signature = msg.getSignature();
        break;
      case SIGNATURE_COMMITMENT :
        partialCommitment = msg.getPartialCommitment();
        break;
      case SIGNATURE_CONTEXT :
        aggregatedCommitment = msg.getAggregatedCommitment();
        participantIndexes = msg.getParticipantIndexes();
        break;
      case HOTSTUFF :
        break;
    }
    int participantIndexesCapacity = Integer.BYTES;
    if (participantIndexes != null) {
      participantIndexesCapacity += participantIndexes.length * Integer.BYTES;
    }

    int capacity = (3 * Integer.BYTES) + nodeBytes.length + justifyBytes.length + nullableByteArrayCapacity(signature) + nullableByteArrayCapacity(partialCommitment)
        + nullableByteArrayCapacity(aggregatedCommitment) + participantIndexesCapacity;
    ByteBuffer buffer = ByteBuffer.allocate(capacity);

    // Encode message fields in a consistent order
    buffer.putInt(msg.getCurrView());
    buffer.putInt(msg.getSenderId());
    buffer.putInt(msg.getType().ordinal());
    buffer.put(nodeBytes);
    buffer.put(justifyBytes);
    putNullableByteArray(buffer, signature);
    putNullableByteArray(buffer, partialCommitment);
    putNullableByteArray(buffer, aggregatedCommitment);
    if (participantIndexes == null) {
      buffer.putInt(NULL_LENGTH);
    } else {
      buffer.putInt(participantIndexes.length);
      for (int participantIndex : participantIndexes) {
        buffer.putInt(participantIndex);
      }
    }

    return buffer.array();
  }
}
