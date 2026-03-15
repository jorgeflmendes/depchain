package pt.ulisboa.depchain.shared.network.packet;

import pt.ulisboa.depchain.proto.DpchPacket;
import pt.ulisboa.depchain.proto.DpchPacketType;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class DpchPacketUtil {
  public static final int MAX_PAYLOAD_LENGTH = 0xFFFF;
  public static final int MAX_PACKET_NUMBER = 0xFFFF;

  private DpchPacketUtil() {
  }

  public static boolean hasType(DpchPacket packet, DpchPacketType packetType) {
    ValidationUtils.requireAllNonNull(ValidationUtils.named("packet", packet), ValidationUtils.named("packetType", packetType));
    if (packetType == DpchPacketType.DPCH_PACKET_TYPE_ACK) {
      return packet.getHasAck() || packet.getPacketType() == DpchPacketType.DPCH_PACKET_TYPE_ACK;
    }
    return packet.getPacketType() == packetType;
  }

  public static DpchPacketType reliableTypeOrNull(DpchPacket packet) {
    ValidationUtils.requireNonNull(packet, "packet");
    return switch (packet.getPacketType()) {
      case DPCH_PACKET_TYPE_DATA, DPCH_PACKET_TYPE_SYN, DPCH_PACKET_TYPE_FIN -> packet.getPacketType();
      default -> null;
    };
  }

  public static byte encodeReliableType(DpchPacketType packetType) {
    ValidationUtils.requireNonNull(packetType, "packetType");
    if (!isReliableType(packetType)) {
      throw new IllegalArgumentException("ACK/UNSPECIFIED cannot be encoded as an acknowledged reliable type");
    }
    return (byte) packetType.getNumber();
  }

  public static DpchPacketType decodeReliableType(byte encodedType) {
    DpchPacketType packetType = DpchPacketType.forNumber(Byte.toUnsignedInt(encodedType));
    if (!isReliableType(packetType)) {
      throw new IllegalArgumentException("Unsupported acknowledged DPCH packet type: " + Byte.toUnsignedInt(encodedType));
    }
    return packetType;
  }

  private static boolean isReliableType(DpchPacketType packetType) {
    return packetType == DpchPacketType.DPCH_PACKET_TYPE_DATA || packetType == DpchPacketType.DPCH_PACKET_TYPE_SYN || packetType == DpchPacketType.DPCH_PACKET_TYPE_FIN;
  }
}
