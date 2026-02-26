package pt.ulisboa.depchain.shared.links.fairloss.message;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.UUID;
import pt.ulisboa.depchain.shared.links.fairloss.codec.BinaryFieldIO;
import pt.ulisboa.depchain.shared.links.fairloss.codec.FairLossMessageCodec;

public record FairLossRequestMessage(UUID requestId, String payload) implements FairLossLinkMessage {
  // Unique message type ID for FairLossRequestMessage, used in encoding/decoding.
  public static final byte MESSAGE_TYPE_ID = 1;

  // Codec for encoding/decoding FairLossRequestMessage instances to/from bytes.
  public static final FairLossMessageCodec<FairLossRequestMessage> CODEC =
      new FairLossMessageCodec<>() {
        @Override
        public byte messageTypeId() {
          return MESSAGE_TYPE_ID;
        }

        @Override
        public Class<FairLossRequestMessage> messageClass() {
          return FairLossRequestMessage.class;
        }

        @Override
        public void writeBody(FairLossRequestMessage message, DataOutputStream output) throws IOException {
          Objects.requireNonNull(message, "message cannot be null");
          Objects.requireNonNull(output, "output cannot be null");

          // UUID | payload (string)
          BinaryFieldIO.writeUuid(output, message.requestId());
          BinaryFieldIO.writeString(output, message.payload());
        }

        @Override
        public FairLossRequestMessage readBody(DataInputStream input) throws IOException {
          Objects.requireNonNull(input, "input cannot be null");
          return new FairLossRequestMessage(BinaryFieldIO.readUuid(input), BinaryFieldIO.readString(input));
        }
      };
  
  public FairLossRequestMessage {
    Objects.requireNonNull(requestId, "requestId cannot be null");
    Objects.requireNonNull(payload, "payload cannot be null");
  }

  @Override
  public byte messageTypeId() {
    return MESSAGE_TYPE_ID;
  }
}
