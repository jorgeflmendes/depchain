package pt.ulisboa.depchain.shared.network.links.handshaked;

import pt.ulisboa.depchain.shared.network.links.AsyncLinkContext;
import pt.ulisboa.depchain.shared.network.links.handshaked.registry.ConnectionStateRegistry;
import pt.ulisboa.depchain.shared.network.links.perfect.PerfectLink;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class HandshakedContext extends AsyncLinkContext<InboundPacket> {
  final PerfectLink perfectLink;
  final ConnectionStateRegistry connectionStateRegistry;

  HandshakedContext(PerfectLink perfectLink) {
    this.perfectLink = ValidationUtils.requireNonNull(perfectLink, "perfectLink");
    this.connectionStateRegistry = new ConnectionStateRegistry();
  }

  void shutdown() {
    shutdownInbox();
    connectionStateRegistry.signalAllStates();
  }
}
