package pt.ulisboa.depchain.server.consensus;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import com.weavechain.curve25519.Scalar;

import pt.ulisboa.depchain.server.consensus.Message.MessageType;
import pt.ulisboa.depchain.server.consensus.threshold.ThresholdSignatureProtocol;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.network.model.ConnectionKey;
import pt.ulisboa.depchain.shared.utils.SerializationUtil;
import pt.ulisboa.depchain.shared.utils.TimeUtil;

public class Replica {
  private ConfigParser config;

  // Transports for node-to-node and client communication.
  private AuthenticatedLink nodeTransport;
  private AuthenticatedLink clientTransport;

  // Clients waiting for responses to close connections after the real command execution.
  private final Map<String, ConnectionKey> clientContexts;

  // Replica config.
  private int id;
  private int n;
  private int f;
  private int quorum;
  private long viewChangeTimeoutMs;

  // Replica state.
  private int viewNumber;
  private QuorumCertificate lockedQC;
  private QuorumCertificate prepareQC;
  private Node currentProposal;
  private Map<String, Node> blockTree;

  // Message queue for incoming consensus or signature messages.
  private BlockingDeque<Message> messageQueue;

  // Pending client commands.
  private Queue<String> pendingCommands;

  // Threshold signature protocol instance.
  private ThresholdSignatureProtocol thresholdProtocol;

  public Replica(int id, ConfigParser config, Scalar localThresholdShare, byte[] publicThresholdKey) {
    this.config = config;
    this.clientContexts = new ConcurrentHashMap<>();

    this.id = id;
    this.n = config.system().n();
    this.f = config.system().f();
    this.quorum = n - f;
    this.viewChangeTimeoutMs = config.timeouts().viewChangeMs();

    this.thresholdProtocol = new ThresholdSignatureProtocol(id, config, localThresholdShare, publicThresholdKey);
    this.lockedQC = thresholdProtocol.genesisQC();
    this.prepareQC = thresholdProtocol.genesisQC();
    this.currentProposal = Node.GENESIS_NODE;
    this.viewNumber = 0;
    this.blockTree = new HashMap<>();

    this.messageQueue = new LinkedBlockingDeque<>();
    this.pendingCommands = new LinkedList<>();

    blockTree.put(Node.GENESIS_NODE.getThisHash(), Node.GENESIS_NODE);
  }

  public void initNetwork(AuthenticatedLink nodeTransport, AuthenticatedLink clientTransport) {
    this.nodeTransport = nodeTransport;
    this.clientTransport = clientTransport;
    this.thresholdProtocol.initTransport(nodeTransport);
  }

  public void run() {
    System.out.println("[Replica " + id + "] starting at view " + viewNumber);

    // Initial view change to start the protocol.
    sendToLeader(new Message(viewNumber, id, MessageType.NEW_VIEW, Node.GENESIS_NODE, prepareQC));

    while (true) {
      try {
        onPrepare();
        onPreCommit();
        onCommit();
        onDecide();
        onNewView();
      } catch (ViewChangeTimeoutException e) {
        onNewView();
      }
    }
  }

  public void receiveMessage(Message msg) {
    messageQueue.add(msg);
  }

  public void receiveCommand(String cmd, ConnectionKey key) {
    pendingCommands.add(cmd);
    this.clientContexts.put(cmd, key);
  }

  private void onPrepare() {
    if (isLeader()) {
      // As leader, gather new_view messages and select the highest QC.
      List<Message> msgs = waitForQuorumMessages(MessageType.NEW_VIEW, viewNumber);
      QuorumCertificate highQC = msgs.get(0).getJustify();
      for (Message m : msgs) {
        if (m.getJustify().getViewNumber() > highQC.getViewNumber()) {
          highQC = m.getJustify();
        }
      }

      // Propose extending the highest QC node with a new command (if any commands).
      String cmd;
      if (pendingCommands.isEmpty()) {
        cmd = "no-op";
      } else {
        cmd = pendingCommands.poll();
      }
      currentProposal = createLeaf(highQC.getNode().getThisHash(), cmd);

      // Broadcast the prepare proposal carrying the chosen high QC.
      broadcast(new Message(viewNumber, id, MessageType.PREPARE, currentProposal, highQC));
    } else {
      // Wait for the prepare message from the leader.
      Message prepareMsg = waitForMessageFromSender(MessageType.PREPARE, viewNumber, getLeader(viewNumber));
      if (prepareMsg == null) {
        return;
      }

      QuorumCertificate justify = prepareMsg.getJustify();
      Node proposedNode = prepareMsg.getNode();

      // Vote if it's justified by a valid QC that safely extends the locked branch.
      boolean hasValidJustify = justify != null && verifyQC(justify);
      boolean extendsJustifiedNode = justify != null && proposedNode != null && proposedNode.extendsNode(justify.getNode());
      boolean isSafeProposal = proposedNode != null && safeNode(proposedNode, justify);
      if (hasValidJustify && extendsJustifiedNode && isSafeProposal) {
        currentProposal = proposedNode;
        sendToLeader(voteMessage(MessageType.PREPARE, currentProposal));
      }
    }
  }

  private void onPreCommit() {
    if (isLeader()) {
      // Aggregate prepare votes into a QC.
      prepareQC = buildQC(MessageType.PREPARE, currentProposal);

      // Pre-commit.
      broadcast(new Message(viewNumber, id, MessageType.PRE_COMMIT, null, prepareQC));
    } else {
      // Wait for the prepare QC and vote.
      Message msg = waitForQCMessage(MessageType.PREPARE, viewNumber, getLeader(viewNumber));
      prepareQC = msg.getJustify();
      sendToLeader(voteMessage(MessageType.PRE_COMMIT, msg.getJustify().getNode()));
    }
  }

  private void onCommit() {
    if (isLeader()) {
      // Aggregate pre commit votes into a QC, lock it, and broadcast commit.
      QuorumCertificate preCommitQC = buildQC(MessageType.PRE_COMMIT, currentProposal);
      lockedQC = preCommitQC;
      broadcast(new Message(viewNumber, id, MessageType.COMMIT, null, preCommitQC));
    } else {
      // Wait for the pre commit QC, update the lock, and vote.
      Message msg = waitForQCMessage(MessageType.PRE_COMMIT, viewNumber, getLeader(viewNumber));
      lockedQC = msg.getJustify();
      sendToLeader(voteMessage(MessageType.COMMIT, msg.getJustify().getNode()));
    }
  }

  private void onDecide() {
    if (isLeader()) {
      // Aggregate commit votes into a QC, broadcast decide and execute.
      QuorumCertificate commitQC = buildQC(MessageType.COMMIT, currentProposal);
      broadcast(new Message(viewNumber, id, MessageType.DECIDE, null, commitQC));
      executeCommand(currentProposal);
    } else {
      // Wait for the commit QC and execute.
      Message msg = waitForQCMessage(MessageType.COMMIT, viewNumber, getLeader(viewNumber));
      executeCommand(msg.getJustify().getNode());
    }
  }

  private void onNewView() {
    // Start a new view locally and notify the new leader.
    viewNumber++;
    sendToLeader(new Message(viewNumber, id, MessageType.NEW_VIEW, null, prepareQC));
  }

  private void executeCommand(Node node) {
    System.out.println("[Replica " + id + "] Executing command: " + node.getCommand());
    blockTree.put(node.getThisHash(), node);

    ConnectionKey key = clientContexts.remove(node.getCommand());
    if (key != null && clientTransport != null) {
      try {
        // Send a response to the client and close the connection.
        byte[] response = ("Received " + node.getCommand()).getBytes(StandardCharsets.UTF_8);
        clientTransport.send(key.connectionId(), response, key.endpoint());
        clientTransport.closeConnection(key.connectionId(), key.endpoint());
        System.out.printf("[Replica " + id + "] Connection %d closed for client %s%n", key.connectionId(), key.endpoint());
      } catch (Exception e) {
        System.err.println("[Replica " + id + "] Error closing client connection: " + e.getMessage());
      }
    }
  }

  private void broadcast(Message msg) {
    byte[] payload = SerializationUtil.encodeMessage(msg);

    for (ConfigParser.ReplicaSection peer : config.replicas()) {
      try {
        InetAddress peerHost = InetAddress.getByName(peer.host());
        InetSocketAddress addr = new InetSocketAddress(peerHost, peer.consensusPort());
        nodeTransport.send(0L, payload, addr); // 0 can be used for inter replica msgs
      } catch (Exception e) {
        System.err.println("[Replica " + id + "] Error in broadcast to " + peer.id() + ": " + e.getMessage());
      }
    }
  }

  private void sendToLeader(Message msg) {
    try {
      // Discover the leader's address from the config.
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
        InetSocketAddress addr = new InetSocketAddress(leaderHost, leader.consensusPort());
        byte[] payload = SerializationUtil.encodeMessage(msg);
        nodeTransport.send(0L, payload, addr); // 0 can be used for inter replica msgs
      } else {
        System.err.println("[Replica " + id + "] Error finding leader config for view " + viewNumber);
      }
    } catch (Exception e) {
      System.err.println("[Replica " + id + "] Error sending message to leader: " + e.getMessage());
    }
  }

  private Message voteMessage(MessageType type, Node node) {
    return thresholdProtocol.createVoteMessage(viewNumber, type, node, getLeader(viewNumber), messageQueue);
  }

  private QuorumCertificate buildQC(MessageType type, Node node) {
    return thresholdProtocol.buildQC(viewNumber, type, node, messageQueue);
  }

  private boolean verifyQC(QuorumCertificate qc) {
    return thresholdProtocol.verifyQC(qc);
  }

  private List<Message> waitForQuorumMessages(MessageType type, int view) {
    // Collect messages from distinct senders until we have a quorum.
    LinkedHashMap<Integer, Message> msgsBySender = new LinkedHashMap<>();
    long deadlineMs = TimeUtil.deadlineAfterNow(viewChangeTimeoutMs);
    while (msgsBySender.size() < quorum) {
      Message msg = pollMessageUntil(deadlineMs, "waiting for " + type + " messages");

      if (msg != null && matchingMSG(msg, type, view) && !thresholdProtocol.isAuxiliaryMessage(msg)) {
        msgsBySender.putIfAbsent(msg.getSenderId(), msg);
      } else {
        messageQueue.addLast(msg);
      }
    }

    return new ArrayList<>(msgsBySender.values());
  }

  private Message waitForMessageFromSender(MessageType type, int view, int sender) {
    long deadlineMs = TimeUtil.deadlineAfterNow(viewChangeTimeoutMs);
    while (true) {
      Message msg = pollMessageUntil(deadlineMs, "waiting for " + type + " from " + sender);
      if (msg != null && msg.getSenderId() == sender && matchingMSG(msg, type, view) && !thresholdProtocol.isAuxiliaryMessage(msg)) {
        return msg;
      } else {
        messageQueue.addLast(msg);
      }
    }
  }

  private Message waitForQCMessage(MessageType type, int view, int sender) {
    long deadlineMs = TimeUtil.deadlineAfterNow(viewChangeTimeoutMs);
    while (true) {
      Message msg = pollMessageUntil(deadlineMs, "waiting for QC " + type);
      if (msg != null && matchingQC(msg.getJustify(), type, view) && msg.getSenderId() == sender && !thresholdProtocol.isAuxiliaryMessage(msg) && verifyQC(msg.getJustify())) {
        return msg;
      }
      messageQueue.addLast(msg);
    }
  }

  private Message pollMessageUntil(long deadlineMs, String description) {
    if (TimeUtil.hasTimedOut(deadlineMs)) {
      throw new ViewChangeTimeoutException("Timed out " + description);
    }

    try {
      // Poll messages until we get one that matches the criteria or we time out.
      long remainingMs = TimeUtil.remainingMsUntil(deadlineMs);
      Message msg = messageQueue.poll(remainingMs, TimeUnit.MILLISECONDS);
      if (msg == null) {
        throw new ViewChangeTimeoutException("Timed out " + description);
      }
      return msg;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ViewChangeTimeoutException("Interrupted while " + description);
    }
  }

  private Node createLeaf(String parentHash, String command) {
    // Extend the parent hash with the command and view number to create a new node hash.
    String thisHash = Integer.toString((parentHash + command + viewNumber).hashCode());
    return new Node(parentHash, thisHash, viewNumber, command);
  }

  private boolean safeNode(Node node, QuorumCertificate qc) {
    if (node == null || qc == null) {
      return false;
    }
    return node.extendsNode(lockedQC.getNode()) || qc.getViewNumber() > lockedQC.getViewNumber();
  }

  private int getLeader(int view) {
    return view % n;
  }

  private boolean isLeader() {
    return getLeader(viewNumber) == id;
  }

  private boolean matchingMSG(Message m, MessageType t, int v) {
    return m.getType() == t && m.getCurrView() == v;
  }

  private boolean matchingQC(QuorumCertificate qc, MessageType t, int v) {
    return qc != null && qc.getType() == t && qc.getViewNumber() == v;
  }
}
