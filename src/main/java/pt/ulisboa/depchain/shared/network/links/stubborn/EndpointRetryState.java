package pt.ulisboa.depchain.shared.network.links.stubborn;

import java.util.HashMap;
import java.util.Map;

import pt.ulisboa.depchain.shared.network.links.LinkFailureException;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedKey;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedMessage;

final class EndpointRetryState {
  public static final int MAX_TRACKED_MESSAGES_PER_ENDPOINT = 1000;
  final Map<TrackedKey, TrackedMessage> trackedMessagesByKey = new HashMap<>(4);
  final Map<TrackedKey, LinkFailureException> terminalFailuresByKey = new HashMap<>(2);

  boolean isEmpty() {
    return trackedMessagesByKey.isEmpty() && terminalFailuresByKey.isEmpty();
  }
}
