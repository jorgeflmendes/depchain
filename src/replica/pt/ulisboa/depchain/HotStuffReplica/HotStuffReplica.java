package pt.ulisboa.depchain.replica;

import java.lang.reflect.Array;
import java.util.List;
import java.util.concurrent.BlockingDeque;

import pt.ulisboa.depchain.QuorumCertificate;
import pt.ulisboa.depchain.message.Message;
import pt.ulisboa.depchain.message.MessageType;
import pt.ulisboa.depchain.node.Node;

public class HotStuffReplica {
    private int id;
    private int n;
    private int f;
    private int quorum;

    private int viewNumber;
    private QuorumCertificate lockedQC;
    private QuorumCertificate prepareQC;
    private Map<String, Node> blockTree;

    private BlockingDeque<Message> messageQueue;
    private Queue<String> pendingCommands;

    public HotStuffReplica(int id, int n) {
        this.id = id;
        this.n = n;
        this.f = (n - 1) / 3;
        this.quorum = n - f;
        this.viewNumber = 0;
        this.lockedQC = null;
        this.prepareQC = null;
        this.blockTree = new HashMap<>();
        this.pendingCommands = new LinkedList<>();
    }

    private boolean matchingMSG(Message m, MessageType t, int v) {
        return m.getType() == t && m.getCurrView() == v;
    }

    private boolean matchingQC(QuorumCertificate qc, MessageType t, int v) {
        return qc.getType() == t && this.getViewNumber() == v;
    }

    private boolean safeNode(Node node, QuorumCertificate qc) {
        return node.getParentHash().equals(lockedQC.getThisHash()) || qc.getViewNumber() > lockedQC.getViewNumber();
    }

    private int getLeader(int view) {
        return view % n;
    }

    private boolean isLeader() {
        return getLeader(viewNumber) == id;
    }

    private void onPrepare() {
        if (isLeader()) {
            ArrayList<Message> msgs = waitForMessages(MessageType.NEW_VIEW, viewNumber - 1);
            QuorumCertificate highQC = msgs.get(0).getJustify();
            for (Message m : msgs) {
                if (m.getJustify().getViewNumber() > highQC.getViewNumber()) {
                    highQC = m.getJustify();
                }
            }

            String cmd;
            if (pendingCommands.isEmpty()) {
                cmd = "no-op";
            } else {
                cmd = pendingCommands.poll();
            }
            Node curProposal = createLeaf(highQC.getNode(), cmd, viewNumber);

            broadcast(new Message(viewNumber, MessageType.PREPARE, curProposal, highQC));
        } else {
            Message prepareMsg = waitForMessage(MessageType.PREPARE, viewNumber, getLeader(viewNumber));
            
        }
    }

    private void onPreCommit(Message msg) {
        if (isLeader()) {

        } else {

        }
    }

    private void onCommit(Message msg) {
        if (isLeader()) {

        } else {

        }
    }

    private void onDecide(Message msg) {
        if (isLeader()) {

        } else {

        }
    }

    private void onNewView(Message msg) {
        if (isLeader()) {

        } else {

        }
    }

    private void broadcast() {

    }

    private void sendtoLeader() {

    }

    private List<Message> waitForMessages(MessageType type, int view) {
        ArrayList<Message> msgs = new ArrayList<>();
        while (msgs.size() < quorum) {
            for (Message msg : messageQueue) {
                if (Message.matchingMSG(msg, type, view)) {
                    msgs.add(msg);
                    messageQueue.remove(msg);
                }
            }
        }
        return msgs;
    }

    private Message waitForMessage(MessageType type, int view, int sender) {
        while (true) {
            for (Message msg : messageQueue) {
                if (Message.matchingMSG(msg, type, view) && msg.getSenderId() == sender) {
                    messageQueue.remove(msg);
                    return msg;
                }
            }
        }
    }
    
    public void run() {
        System.out.println("[Replica " + id + "] starting at view " + viewNumber);

        // On boot send NEW_VIEW to the first leader
        sendToLeader();

        while (true) {
            onPrepare();
            onPreCommit();
            onCommit();
            onDecide();
            onNewView();
        }
    }

    public static void main(String[] args) {
        // Initialize the replica and start the protocol
        int id = Integer.parseInt(args[0]);
        int n = Integer.parseInt(args[1]);
        
        HotStuffReplica replica = new HotStuffReplica(id, n);

        replica.run();
    }
}
