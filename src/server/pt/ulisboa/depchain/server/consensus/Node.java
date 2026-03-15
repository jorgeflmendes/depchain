package pt.ulisboa.depchain.server.consensus;

public class Node {
  public static final Node GENESIS_NODE = new Node("0", "0", 0, NodeCommand.GENESIS);

  private String thisHash;
  private String parentHash;
  private int viewNumber;
  private NodeCommand command;

  public Node(String parentHash, String thisHash, int viewNumber, String command) {
    this(parentHash, thisHash, viewNumber, new NodeCommand(command, null));
  }

  public Node(String parentHash, String thisHash, int viewNumber, String command, ClientRequestId clientRequestId) {
    this(parentHash, thisHash, viewNumber, new NodeCommand(command, clientRequestId));
  }

  public Node(String parentHash, String thisHash, int viewNumber, NodeCommand command) {
    this.parentHash = parentHash;
    this.thisHash = thisHash;
    this.viewNumber = viewNumber;
    this.command = command;
  }

  public boolean extendsNode(Node other) {
    return this.parentHash.equals(other.getThisHash());
  }

  public String getThisHash() {
    return thisHash;
  }

  public String getParentHash() {
    return parentHash;
  }

  public int getViewNumber() {
    return viewNumber;
  }

  public NodeCommand getCommand() {
    return command;
  }
}
