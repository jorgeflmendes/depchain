package pt.ulisboa.depchain.shared.links.fairloss.codec;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import pt.ulisboa.depchain.shared.links.fairloss.message.FairLossLinkMessage;

public interface FairLossMessageCodec<T extends FairLossLinkMessage> {
  // Get the unique message type ID associated with this codec.
  byte messageTypeId();

  // Get the Java class of the message type handled by this codec.
  Class<T> messageClass();

  // Write the message body (excluding the header) to the output stream.
  void writeBody(T message, DataOutputStream output) throws IOException;

  // Read the message body (excluding the header) from the input stream and return the decoded message.
  T readBody(DataInputStream input) throws IOException;
}
