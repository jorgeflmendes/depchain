package pt.ulisboa.depchain.shared.network.links.stubborn;

import java.io.IOException;

import pt.ulisboa.depchain.shared.network.links.fairloss.InboundBytes;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

final class StubbornReceiver {
  private final StubbornContext context;

  StubbornReceiver(StubbornContext context) {
    this.context = ValidationUtils.requireNonNull(context, "context");
  }

  // Receives a message, blocking until one is available or the receiver is closed.
  InboundBytes receive() throws IOException {
    context.ensureOpen();
    return context.fairLossLink.receive();
  }

  // Receives a message with a timeout, returning null if the timeout expires or the receiver is
  // closed.
  InboundBytes receive(long timeoutMs) throws IOException {
    context.ensureOpen();
    return context.fairLossLink.receive(timeoutMs);
  }
}
