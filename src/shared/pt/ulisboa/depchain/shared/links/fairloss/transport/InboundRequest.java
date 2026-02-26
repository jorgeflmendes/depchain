package pt.ulisboa.depchain.shared.links.fairloss.transport;

import java.net.InetAddress;
import java.util.Objects;
import pt.ulisboa.depchain.shared.links.fairloss.message.FairLossRequestMessage;

public final class InboundRequest {
  private final FairLossRequestMessage request;
  private final InetAddress senderIp;
  private final int senderPort;

  public InboundRequest(FairLossRequestMessage request, InetAddress senderIp, int senderPort) {
    Objects.requireNonNull(request, "request cannot be null");
    Objects.requireNonNull(senderIp, "senderIp cannot be null");
    if (senderPort < 1 || senderPort > 65535) {
      throw new IllegalArgumentException("senderPort must be in range [1, 65535]");
    }

    this.request = request;
    this.senderIp = senderIp;
    this.senderPort = senderPort;
  }
  
  public FairLossRequestMessage request() {
    return request;
  }

  public InetAddress senderIp() {
    return senderIp;
  }

  public int senderPort() {
    return senderPort;
  }
}
