package pt.ulisboa.depchain.server.consensus;

import java.util.Arrays;

import pt.ulisboa.depchain.shared.model.ClientRequest;

public class Message {
  // Hotstuff message types.
  public enum MessageType {
    NEW_VIEW, PREPARE, PRE_COMMIT, COMMIT, DECIDE, FORWARDED_REQUEST
  }

  // Threshold signature payload types.
  public enum ThresholdPayloadType {
    HOTSTUFF, SIGNATURE_SHARE, SIGNATURE_COMMITMENT, SIGNATURE_CONTEXT
  }

  // Hotstuff things.
  private int currView;
  private int senderId;
  private MessageType type;
  private Node node;
  private QuorumCertificate justify;
  private ClientRequest forwardedRequest;

  // Threshold signature things.
  private ThresholdPayloadType thresholdPayloadType; // It can be nothing (hotstuff stuff), a signature share, a commitment, or the full context.
  private byte[] signature;
  private byte[] partialCommitment;
  private byte[] aggregatedCommitment;
  private int[] participantIndexes;

  public Message(int currView, int senderId, MessageType type, Node node, QuorumCertificate justify) {
    this.currView = currView;
    this.senderId = senderId;
    this.type = type;
    this.node = node;
    this.justify = justify;
    this.forwardedRequest = null;
    this.thresholdPayloadType = ThresholdPayloadType.HOTSTUFF;
  }

  public Message(int currView, int senderId, MessageType type, ClientRequest forwardedRequest) {
    this.currView = currView;
    this.senderId = senderId;
    this.type = type;
    this.node = null;
    this.justify = null;
    this.forwardedRequest = forwardedRequest;
    this.thresholdPayloadType = ThresholdPayloadType.HOTSTUFF;
  }

  public int getCurrView() {
    return currView;
  }

  public int getSenderId() {
    return senderId;
  }

  public MessageType getType() {
    return type;
  }

  public Node getNode() {
    return node;
  }

  public QuorumCertificate getJustify() {
    return justify;
  }

  public ClientRequest getForwardedRequest() {
    return forwardedRequest;
  }

  public ThresholdPayloadType getThresholdPayloadType() {
    return thresholdPayloadType;
  }

  public void setSignature(byte[] sig) {
    if (sig == null) {
      thresholdPayloadType = ThresholdPayloadType.HOTSTUFF;
      signature = null;
      partialCommitment = null;
      aggregatedCommitment = null;
      participantIndexes = null;
      return;
    }

    thresholdPayloadType = ThresholdPayloadType.SIGNATURE_SHARE;
    signature = Arrays.copyOf(sig, sig.length);
    partialCommitment = null;
    aggregatedCommitment = null;
    participantIndexes = null;
  }

  public byte[] getSignature() {
    if (getThresholdPayloadType() == ThresholdPayloadType.SIGNATURE_SHARE) {
      return Arrays.copyOf(signature, signature.length);
    }

    return null;
  }

  public void setPartialCommitment(byte[] commitment) {
    if (commitment == null) {
      thresholdPayloadType = ThresholdPayloadType.HOTSTUFF;
      signature = null;
      partialCommitment = null;
      aggregatedCommitment = null;
      participantIndexes = null;
      return;
    }

    thresholdPayloadType = ThresholdPayloadType.SIGNATURE_COMMITMENT;
    signature = null;
    partialCommitment = Arrays.copyOf(commitment, commitment.length);
    aggregatedCommitment = null;
    participantIndexes = null;
  }

  public byte[] getPartialCommitment() {
    if (getThresholdPayloadType() == ThresholdPayloadType.SIGNATURE_COMMITMENT) {
      return Arrays.copyOf(partialCommitment, partialCommitment.length);
    }

    return null;
  }

  public void setThresholdContext(byte[] aggregatedCommitment, int[] participantIndexes) {
    if (aggregatedCommitment == null && participantIndexes == null) {
      thresholdPayloadType = ThresholdPayloadType.HOTSTUFF;
      signature = null;
      partialCommitment = null;
      this.aggregatedCommitment = null;
      this.participantIndexes = null;
      return;
    }

    if (aggregatedCommitment == null || participantIndexes == null) {
      throw new IllegalArgumentException("Threshold context requires both aggregatedCommitment and participantIndexes");
    }

    thresholdPayloadType = ThresholdPayloadType.SIGNATURE_CONTEXT;
    signature = null;
    partialCommitment = null;
    this.aggregatedCommitment = Arrays.copyOf(aggregatedCommitment, aggregatedCommitment.length);
    this.participantIndexes = Arrays.copyOf(participantIndexes, participantIndexes.length);
  }

  public byte[] getAggregatedCommitment() {
    if (getThresholdPayloadType() == ThresholdPayloadType.SIGNATURE_CONTEXT) {
      return Arrays.copyOf(aggregatedCommitment, aggregatedCommitment.length);
    }

    return null;
  }

  public int[] getParticipantIndexes() {
    if (getThresholdPayloadType() == ThresholdPayloadType.SIGNATURE_CONTEXT) {
      return Arrays.copyOf(participantIndexes, participantIndexes.length);
    }

    return null;
  }
}
