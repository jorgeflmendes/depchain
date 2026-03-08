package pt.ulisboa.depchain.message;

import pt.ulisboa.depchain.node.Node;
import pt.ulisboa.depchain.certificate.QuorumCertificate;

public class Message {

    public enum MessageType {
        NEW_VIEW,
        PREPARE,
        PRE_COMMIT,
        COMMIT,
        DECIDE
    }

    private int currView;
    private MessageType type;
    private Node node;
    private QuorumCertificate justify;
    private byte[] partialSig;
    private int senderId;

    public Message(int currView, MessageType type, Node node) {
        this.currView = currView;
        this.type = type;
        this.node = node;
    }

    public int getCurrView() {
        return currView;
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

    public byte[] getPartialSig() {
        return partialSig;
    }

    public VoteMessage() {
    }

    
}
