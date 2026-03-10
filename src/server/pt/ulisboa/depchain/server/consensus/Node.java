package pt.ulisboa.depchain.server.consensus;

public class Node {
  public static final Node GENESIS_NODE = new Node("0", "0", 0, "GENESIS");

  private String thisHash;
  private String parentHash;
  private int viewNumber;
  private String command;

  public Node(String parentHash, String thisHash, int viewNumber, String command) {
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

  public String getCommand() {
    return command;
  }
}
