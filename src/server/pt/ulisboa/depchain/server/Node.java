package pt.ulisboa.depchain.server;

public class Node {
  private String thisHash;
  private String parentHash;
  private int viewNumber;
  private String command;

  public static final Node GENESIS_NODE = new Node("0", "0", 0, "GENESIS");

  public Node(String parentHash, String thisHash, int viewNumber, String command) {
    this.parentHash = parentHash;
    this.thisHash = thisHash;
    this.viewNumber = viewNumber;
    this.command = command;
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

  public boolean extendsNode(Node other) {
    return this.parentHash.equals(other.getThisHash());
  }
}
