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
    private int senderId;
    private MessageType type;
    private Node node;
    private QuorumCertificate justify;
    private byte[] signature;

    public Message(int currView, int senderId, MessageType type, Node node, QuorumCertificate justify) {
        this.currView = currView;
        this.senderId = senderId;
        this.type = type;
        this.node = node;
        this.justify = justify;
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

    private void signMessage(byte[] sig) {
        this.signature = sig;
    }
}
