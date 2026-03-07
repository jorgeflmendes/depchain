package pt.ulisboa.depchain.shared.network.links.stubborn.tracking;

import pt.ulisboa.depchain.shared.utils.ValidationUtils;

// Represents the unique key for tracking a message.
public record TrackedKey(long connectionId, int sequenceNumber, int packetTypeCode) {
  public TrackedKey {
    ValidationUtils.requireInClosedRangeInt(packetTypeCode, 0, 0xFF, "packetTypeCode");
  }
}
