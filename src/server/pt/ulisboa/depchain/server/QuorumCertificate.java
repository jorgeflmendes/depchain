package pt.ulisboa.depchain.server;

import java.util.Arrays;

import pt.ulisboa.depchain.server.Message.MessageType;

public class QuorumCertificate {
  private MessageType type;
  private int viewNumber;
  private Node node;
  private byte[] aggregatedSignature;

  public QuorumCertificate(MessageType type, int viewNumber, Node node) {
    this.type = type;
    this.viewNumber = viewNumber;
    this.node = node;
  }

  public MessageType getType() {
    return type;
  }

  public int getViewNumber() {
    return viewNumber;
  }

  public Node getNode() {
    return node;
  }

  public byte[] getAggregatedSignature() {
    if (aggregatedSignature == null) {
      return null;
    }
    return Arrays.copyOf(aggregatedSignature, aggregatedSignature.length);
  }

  public void setAggregatedSignature(byte[] sig) {
    if (sig == null) {
      aggregatedSignature = null;
      return;
    }
    aggregatedSignature = Arrays.copyOf(sig, sig.length);
  }
}
