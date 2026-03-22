package pt.ulisboa.depchain.client.shell;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.junit.jupiter.api.Test;

class DpchClientShellArgsTest {
  @Test
  void unknownInputIsTokenizedAsEntered() {
    assertArrayEquals(new String[]{"hello", "world"}, DpchClientShell.shellArgs("hello world"));
    assertArrayEquals(new String[]{"unknown", "hello", "world"}, DpchClientShell.shellArgs("unknown hello world"));
  }

  @Test
  void exitAliasesNormalizeToExit() {
    assertArrayEquals(new String[]{"exit"}, DpchClientShell.shellArgs("EXIT"));
    assertArrayEquals(new String[]{"quit"}, DpchClientShell.shellArgs("quit"));
  }

  @Test
  void helpFlagsMapToHelp() {
    assertArrayEquals(new String[]{"help"}, DpchClientShell.shellArgs("help"));
    assertArrayEquals(new String[]{"help", "depcoin-transfer"}, DpchClientShell.shellArgs("help depcoin-transfer"));
    assertArrayEquals(new String[]{"?"}, DpchClientShell.shellArgs("?"));
    assertArrayEquals(new String[]{"--help"}, DpchClientShell.shellArgs("--help"));
  }

  @Test
  void canonicalCommandsStayCanonical() {
    assertArrayEquals(new String[]{"depcoin-transfer", "abc", "10", "0"}, DpchClientShell.shellArgs("depcoin-transfer abc 10 0"));
    assertArrayEquals(new String[]{"transfer", "abc", "10", "0"}, DpchClientShell.shellArgs("transfer abc 10 0"));
    assertArrayEquals(new String[]{"depcoin-balance"}, DpchClientShell.shellArgs("depcoin-balance"));
    assertArrayEquals(new String[]{"b", "abc"}, DpchClientShell.shellArgs("b abc"));
    assertArrayEquals(new String[]{"dep-balance", "abc"}, DpchClientShell.shellArgs("dep-balance abc"));
    assertArrayEquals(new String[]{"ist-balance", "abc"}, DpchClientShell.shellArgs("ist-balance abc"));
    assertArrayEquals(new String[]{"ist-balance"}, DpchClientShell.shellArgs("ist-balance"));
    assertArrayEquals(new String[]{"my-address"}, DpchClientShell.shellArgs("my-address"));
    assertArrayEquals(new String[]{"address"}, DpchClientShell.shellArgs("address"));
    assertArrayEquals(new String[]{"addr"}, DpchClientShell.shellArgs("addr"));
    assertArrayEquals(new String[]{"ist-transfer", "abc", "15", "5"}, DpchClientShell.shellArgs("ist-transfer abc 15 5"));
    assertArrayEquals(new String[]{"it", "abc", "15", "5"}, DpchClientShell.shellArgs("it abc 15 5"));
    assertArrayEquals(new String[]{"contract-call", "abc", "deadbeef", "5"}, DpchClientShell.shellArgs("contract-call abc deadbeef 5"));
    assertArrayEquals(new String[]{"cc", "abc", "deadbeef", "5"}, DpchClientShell.shellArgs("cc abc deadbeef 5"));
    assertArrayEquals(new String[]{"q"}, DpchClientShell.shellArgs("q"));
  }
}
