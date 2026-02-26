package pt.ulisboa.depchain.shared.links.fairloss.message;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.UUID;
import pt.ulisboa.depchain.shared.links.fairloss.codec.BinaryFieldIO;
import pt.ulisboa.depchain.shared.links.fairloss.codec.FairLossMessageCodec;

public record FairLossResponseMessage(UUID requestId, boolean success, String payload) implements FairLossLinkMessage {
  // Unique message type ID for FairLossResponseMessage, used in encoding/decoding.
  public static final byte MESSAGE_TYPE_ID = 2;

  // Codec for encoding/decoding FairLossResponseMessage instances to/from bytes.
  public static final FairLossMessageCodec<FairLossResponseMessage> CODEC =
      new FairLossMessageCodec<>() {
        @Override
        public byte messageTypeId() {
          return MESSAGE_TYPE_ID;
        }

        @Override
        public Class<FairLossResponseMessage> messageClass() {
          return FairLossResponseMessage.class;
        }

        @Override
        public void writeBody(FairLossResponseMessage message, DataOutputStream output) throws IOException {
          Objects.requireNonNull(message, "message cannot be null");
          Objects.requireNonNull(output, "output cannot be null");

          // UUID | success (boolean) | payload (string)
          BinaryFieldIO.writeUuid(output, message.requestId());
          output.writeBoolean(message.success());
          BinaryFieldIO.writeString(output, message.payload());
        }

        @Override
        public FairLossResponseMessage readBody(DataInputStream input) throws IOException {
          Objects.requireNonNull(input, "input cannot be null");
          return new FairLossResponseMessage(
              BinaryFieldIO.readUuid(input), input.readBoolean(), BinaryFieldIO.readString(input));
        }
      };

  public FairLossResponseMessage {
    Objects.requireNonNull(requestId, "requestId cannot be null");
    Objects.requireNonNull(payload, "payload cannot be null");
  }

  @Override
  public byte messageTypeId() {
    return MESSAGE_TYPE_ID;
  }
}
