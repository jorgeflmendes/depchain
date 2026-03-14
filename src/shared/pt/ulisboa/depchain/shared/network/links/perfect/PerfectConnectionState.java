package pt.ulisboa.depchain.shared.network.links.perfect;

final class PerfectConnectionState {
  private final SenderState senderState = new SenderState();
  private final ReceiverState receiverState = new ReceiverState(0);

  SenderState senderState() {
    return senderState;
  }

  ReceiverState receiverState() {
    return receiverState;
  }
}
