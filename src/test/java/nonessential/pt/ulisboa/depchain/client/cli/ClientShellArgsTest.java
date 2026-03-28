package pt.ulisboa.depchain.client.cli;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.junit.jupiter.api.Test;

class ClientShellArgsTest {
  @Test
  void unknownInputIsTokenizedAsEntered() {
    assertArrayEquals(new String[]{"hello", "world"}, ClientShell.shellArgs("hello world"));
    assertArrayEquals(new String[]{"unknown", "hello", "world"}, ClientShell.shellArgs("unknown hello world"));
  }

  @Test
  void exitAliasesNormalizeToExit() {
    assertArrayEquals(new String[]{"exit"}, ClientShell.shellArgs("EXIT"));
    assertArrayEquals(new String[]{"quit"}, ClientShell.shellArgs("quit"));
  }

  @Test
  void helpFlagsMapToHelp() {
    assertArrayEquals(new String[]{"help"}, ClientShell.shellArgs("help"));
    assertArrayEquals(new String[]{"help", "depcoin-transfer"}, ClientShell.shellArgs("help depcoin-transfer"));
    assertArrayEquals(new String[]{"?"}, ClientShell.shellArgs("?"));
    assertArrayEquals(new String[]{"--help"}, ClientShell.shellArgs("--help"));
  }

  @Test
  void canonicalCommandsStayCanonical() {
    assertArrayEquals(new String[]{"depcoin-transfer", "abc", "10", "0"}, ClientShell.shellArgs("depcoin-transfer abc 10 0"));
    assertArrayEquals(new String[]{"transfer", "abc", "10", "0"}, ClientShell.shellArgs("transfer abc 10 0"));
    assertArrayEquals(new String[]{"depcoin-balance"}, ClientShell.shellArgs("depcoin-balance"));
    assertArrayEquals(new String[]{"b", "abc"}, ClientShell.shellArgs("b abc"));
    assertArrayEquals(new String[]{"dep-balance", "abc"}, ClientShell.shellArgs("dep-balance abc"));
    assertArrayEquals(new String[]{"ist-balance", "abc"}, ClientShell.shellArgs("ist-balance abc"));
    assertArrayEquals(new String[]{"ist-balance"}, ClientShell.shellArgs("ist-balance"));
    assertArrayEquals(new String[]{"my-address"}, ClientShell.shellArgs("my-address"));
    assertArrayEquals(new String[]{"address"}, ClientShell.shellArgs("address"));
    assertArrayEquals(new String[]{"addr"}, ClientShell.shellArgs("addr"));
    assertArrayEquals(new String[]{"ist-transfer", "abc", "15", "5"}, ClientShell.shellArgs("ist-transfer abc 15 5"));
    assertArrayEquals(new String[]{"it", "abc", "15", "5"}, ClientShell.shellArgs("it abc 15 5"));
    assertArrayEquals(new String[]{"contract-call", "abc", "deadbeef", "5"}, ClientShell.shellArgs("contract-call abc deadbeef 5"));
    assertArrayEquals(new String[]{"cc", "abc", "deadbeef", "5"}, ClientShell.shellArgs("cc abc deadbeef 5"));
    assertArrayEquals(new String[]{"q"}, ClientShell.shellArgs("q"));
  }
}
