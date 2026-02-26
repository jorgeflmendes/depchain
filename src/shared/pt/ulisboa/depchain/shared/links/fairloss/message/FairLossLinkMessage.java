package pt.ulisboa.depchain.shared.links.fairloss.message;

public interface FairLossLinkMessage {
  // Get the unique message type ID associated with this message type, used for encoding/decoding.
  byte messageTypeId();
}
