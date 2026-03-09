package pt.ulisboa.depchain.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import pt.ulisboa.depchain.server.Message.MessageType;

public class Replica {
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

  public Replica(int id, int n) {
    this.id = id;
    this.n = n;
    this.f = (n - 1) / 3;
    this.quorum = n - f;
    this.viewNumber = 0;
    this.lockedQC = null;
    this.prepareQC = null;
    this.blockTree = new HashMap<>();
    this.messageQueue = new LinkedBlockingDeque<>();
    this.pendingCommands = new LinkedList<>();

    blockTree.put(Node.GENESIS_NODE.getThisHash(), Node.GENESIS_NODE);
  }

  private Message voteMessage(MessageType type, Node node, QuorumCertificate qc) {
    Message msg = new Message(viewNumber, id, type, node, qc);
    // use threshold signatures to sign the message
    return msg;
  }

  private Node createLeaf(String parentHash, String command) {
    String thisHash = Integer.toString((parentHash + command + viewNumber).hashCode());
    return new Node(parentHash, thisHash, viewNumber, command);
  }

  private boolean matchingMSG(Message m, MessageType t, int v) {
    return m.getType() == t && m.getCurrView() == v;
  }

  private boolean matchingQC(QuorumCertificate qc, MessageType t, int v) {
    return qc.getType() == t && qc.getViewNumber() == v;
  }

  private boolean safeNode(Node node, QuorumCertificate qc) {
    return node.extendsNode(lockedQC.getNode()) || qc.getViewNumber() > lockedQC.getViewNumber();
  }

  private int getLeader(int view) {
    return view % n;
  }

  private boolean isLeader() {
    return getLeader(viewNumber) == id;
  }

  private void onPrepare() {
    if (isLeader()) {
      List<Message> msgs = waitForMessages(MessageType.NEW_VIEW, viewNumber - 1);
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
      Node curProposal = createLeaf(highQC.getNode().getThisHash(), cmd);

      broadcast(new Message(viewNumber, id, MessageType.PREPARE, curProposal, highQC));
    } else {
      Message prepareMsg = waitForMessage(MessageType.PREPARE, viewNumber, getLeader(viewNumber));
      if (prepareMsg.getNode().extendsNode(prepareMsg.getJustify().getNode()) && safeNode(prepareMsg.getNode(), prepareMsg.getJustify())) {
        sendtoLeader(voteMessage(MessageType.PREPARE, prepareMsg.getNode(), null));
      }
    }
  }

  private void onPreCommit() {
    if (isLeader()) {
      List<Message> msgs = waitForMessages(MessageType.PREPARE, viewNumber);
      prepareQC = null; // combine signatures and create QC
      broadcast(new Message(viewNumber, id, MessageType.PRE_COMMIT, null, prepareQC));
    } else {
      Message msg = waitForQC(MessageType.PREPARE, viewNumber, getLeader(viewNumber));
      prepareQC = msg.getJustify();
      sendtoLeader(voteMessage(MessageType.PRE_COMMIT, msg.getJustify().getNode(), null));
    }
  }

  private void onCommit() {
    if (isLeader()) {
      waitForMessages(MessageType.PRE_COMMIT, viewNumber);
      QuorumCertificate preCommitQC = null; // combine signatures and create QC
      broadcast(new Message(viewNumber, id, MessageType.COMMIT, null, lockedQC));
    } else {
      Message msg = waitForQC(MessageType.PRE_COMMIT, viewNumber, getLeader(viewNumber));
      lockedQC = msg.getJustify();
      sendtoLeader(voteMessage(MessageType.COMMIT, msg.getJustify().getNode(), null));
    }
  }

  private void onDecide() {
    if (isLeader()) {
      waitForMessages(MessageType.COMMIT, viewNumber);
      QuorumCertificate commitQC = null; // combine signatures and create QC
      broadcast(new Message(viewNumber, id, MessageType.DECIDE, null, commitQC));
    } else {
      Message msg = waitForQC(MessageType.COMMIT, viewNumber, getLeader(viewNumber));
      executeCommand(msg.getJustify().getNode());
    }
  }

  private void onNewView() {
    viewNumber++;
    sendtoLeader(new Message(viewNumber, id, MessageType.NEW_VIEW, null, prepareQC));
  }

  private void broadcast(Message msg) {
  }

  private void sendtoLeader(Message msg) {
  }

  private List<Message> waitForMessages(MessageType type, int view) {
    ArrayList<Message> msgs = new ArrayList<>();
    while (msgs.size() < quorum) {
      for (Message msg : messageQueue) {
        if (matchingMSG(msg, type, view)) {
          msgs.add(msg);
          messageQueue.remove(msg);
        }
      }
    }
    return msgs;
  }

  private Message waitForQC(MessageType type, int view, int sender) {
    while (true) {
      for (Message msg : messageQueue) {
        if (matchingQC(msg.getJustify(), type, view) && msg.getSenderId() == sender) {
          messageQueue.remove(msg);
          return msg;
        }
      }
    }
  }

  private Message waitForMessage(MessageType type, int view, int sender) {
    while (true) {
      for (Message msg : messageQueue) {
        if (matchingMSG(msg, type, view) && msg.getSenderId() == sender) {
          messageQueue.remove(msg);
          return msg;
        }
      }
    }
  }

  private void executeCommand(Node node) {
    System.out.println("[Replica " + id + "] Executing command: " + node.getCommand());
    blockTree.put(node.getThisHash(), node);
  }

  public void run() {
    System.out.println("[Replica " + id + "] starting at view " + viewNumber);

    // Start the protocol by sending a new view message to the leader
    // sendtoLeader(new Message());

    while (true) {
      onPrepare();
      onPreCommit();
      onCommit();
      onDecide();
      onNewView();
    }
  }

  public void receiveMessage(Message msg) {
    messageQueue.add(msg);
  }

  public void receiveCommand(String cmd) {
    pendingCommands.add(cmd);
  }

  public static void main(String[] args) {
    int id = Integer.parseInt(args[0]);
    int n = Integer.parseInt(args[1]);

    Replica replica = new Replica(id, n);
    replica.run();
  }
}
