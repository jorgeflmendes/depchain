package pt.ulisboa.depchain.server;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import pt.ulisboa.depchain.server.Message.MessageType;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.utils.SerializationUtil;

public class Replica {
  private AuthenticatedLink nodeTransport;
  private AuthenticatedLink clientTransport;
  private ConfigParser config;
  private final Map<String, ConnectionKey> clientContexts;

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

  private PrivateKey privateKey;
  private Map<Long, PublicKey> publicKeys;

  private static final long VIEW_TIMEOUT_MS = 5000;
  private volatile boolean viewTimedOut;
  private Thread timerThread;

  public Replica(int id, int n, ConfigParser config) {
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
    this.config = config;
    this.clientContexts = new java.util.concurrent.ConcurrentHashMap<>();

    blockTree.put(Node.GENESIS_NODE.getThisHash(), Node.GENESIS_NODE);
  }

  public void initNetwork(AuthenticatedLink nodeTransport, AuthenticatedLink clientTransport) {
    this.nodeTransport = nodeTransport;
    this.clientTransport = clientTransport;
  }

  public void initCrypto(PrivateKey privKey, Map<Long, PublicKey> pubKeys) {
    this.privateKey = privKey;
    this.publicKeys = pubKeys;
}

  private Message voteMessage(MessageType type, Node node, QuorumCertificate qc) {
    Message msg = new Message(viewNumber, id, type, node, qc);
    // TODO: use threshold signatures to sign the message
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

  private void nextView(int view) {
    viewTimedOut = false;
    Thread caller = Thread.currentThread();
    timerThread = new Thread(() -> {
      try {
        Thread.sleep(VIEW_TIMEOUT_MS);
        viewTimedOut = true;
        caller.interrupt();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    timerThread.setDaemon(true);
    timerThread.start();
  }

  private void cancelTimer() {
    if (timerThread != null) {
      timerThread.interrupt();
    }
  }

  private void onPrepare() throws InterruptedException {
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

  private void onPreCommit() throws InterruptedException {
    if (isLeader()) {
      List<Message> msgs = waitForMessages(MessageType.PREPARE, viewNumber);
      prepareQC = null; // TODO: combine signatures and create QC
      broadcast(new Message(viewNumber, id, MessageType.PRE_COMMIT, null, prepareQC));
    } else {
      Message msg = waitForQC(MessageType.PREPARE, viewNumber, getLeader(viewNumber));
      prepareQC = msg.getJustify();
      sendtoLeader(voteMessage(MessageType.PRE_COMMIT, msg.getJustify().getNode(), null));
    }
  }

  private void onCommit() throws InterruptedException {
    if (isLeader()) {
      waitForMessages(MessageType.PRE_COMMIT, viewNumber);
      QuorumCertificate preCommitQC = null; // TODO: combine signatures and create QC
      broadcast(new Message(viewNumber, id, MessageType.COMMIT, null, lockedQC));
    } else {
      Message msg = waitForQC(MessageType.PRE_COMMIT, viewNumber, getLeader(viewNumber));
      lockedQC = msg.getJustify();
      sendtoLeader(voteMessage(MessageType.COMMIT, msg.getJustify().getNode(), null));
    }
  }

  private void onDecide() throws InterruptedException {
    if (isLeader()) {
      waitForMessages(MessageType.COMMIT, viewNumber);
      QuorumCertificate commitQC = null; // TODO: combine signatures and create QC
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
    byte[] payload = SerializationUtil.encodeMessage(msg);

    for (ConfigParser.ReplicaSection peer : config.replicas()) {
      try {
        InetAddress peerHost = InetAddress.getByName(peer.host());
        java.net.InetSocketAddress addr = new java.net.InetSocketAddress(peerHost, peer.consensusPort());
        nodeTransport.send(0L, payload, addr);
      } catch (Exception e) {
        System.err.println("Error in broadcast to " + peer.id() + ": " + e.getMessage());
      }
    }
  }

  private void sendtoLeader(Message msg) {
    try {
      byte[] payload = SerializationUtil.encodeMessage(msg);
      int leaderId = getLeader(viewNumber);

      ConfigParser.ReplicaSection leader = null;
      for (ConfigParser.ReplicaSection r : config.replicas()) {
        if (r.senderId() == leaderId) {
          leader = r;
          break;
        }
      }

      if (leader != null) {
        InetAddress leaderHost = InetAddress.getByName(leader.host());
        java.net.InetSocketAddress addr = new java.net.InetSocketAddress(leaderHost, leader.consensusPort());
        nodeTransport.send(0L, payload, addr);
      } else {
        System.err.println("Error finding leader config for view " + viewNumber);
      }
    } catch (Exception e) {
      System.err.println("Error sending message to leader: " + e.getMessage());
    }
  }

  private List<Message> waitForMessages(MessageType type, int view) throws InterruptedException {
    nextView(view);
    ArrayList<Message> msgs = new ArrayList<>();
    while (msgs.size() < quorum) {
      if (viewTimedOut) throw new InterruptedException("View " + view + " timed out");
      for (Message msg : messageQueue) {
        if (matchingMSG(msg, type, view)) {
          msgs.add(msg);
          messageQueue.remove(msg);
        }
      }
    }
    cancelTimer();
    return msgs;
  }

  private Message waitForQC(MessageType type, int view, int sender) throws InterruptedException {
    nextView(view);
    while (true) {
      if (viewTimedOut) throw new InterruptedException("View " + view + " timed out");
      for (Message msg : messageQueue) {
        if (matchingQC(msg.getJustify(), type, view) && msg.getSenderId() == sender) {
          messageQueue.remove(msg);
          cancelTimer();
          return msg;
        }
      }
    }
  }

  private Message waitForMessage(MessageType type, int view, int sender) throws InterruptedException {
    nextView(view);
    while (true) {
      Message msg = messageQueue.take();
      if (matchingMSG(msg, type, view) && msg.getSenderId() == sender) {
        cancelTimer();
        return msg;
      } else {
        messageQueue.addLast(msg);
      }
    }
  }

  private void executeCommand(Node node) {
    System.out.println("[Replica " + id + "] Executing command: " + node.getCommand());
    blockTree.put(node.getThisHash(), node);

    // Send final response to client and close connection
    ConnectionKey key = clientContexts.remove(node.getCommand());
    if (key != null && clientTransport != null) {
      try {
        byte[] response = "Success".getBytes(StandardCharsets.UTF_8);
        clientTransport.send(key.connectionId(), response, key.endpoint());
        clientTransport.closeConnection(key.connectionId(), key.endpoint());

        System.out.printf("[Replica %d] Connection %d closed for client %s%n", id, key.connectionId(), key.endpoint());
      } catch (Exception e) {
        System.err.println("Error closing client connection: " + e.getMessage());
      }
    }
  }

  public void run() {
    System.out.println("[Replica " + id + "] starting at view " + viewNumber);

    sendtoLeader(new Message(viewNumber, id, MessageType.NEW_VIEW, Node.GENESIS_NODE, prepareQC));
    viewNumber = 1;

    while (true) {
      try {
        onPrepare();
        onPreCommit();
        onCommit();
        onDecide();
      } catch (InterruptedException e) {
        System.out.println("[Replica " + id + "] view " + viewNumber + " timed out");
        cancelTimer();
        Thread.interrupted();
      }
      onNewView();
    }
  }

  public void receiveMessage(Message msg) {
    messageQueue.add(msg);
  }

  public void receiveCommand(String cmd, ConnectionKey key) {
    pendingCommands.add(cmd);
    this.clientContexts.put(cmd, key);
  }
}
