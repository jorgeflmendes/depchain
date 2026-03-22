package pt.ulisboa.depchain.client;

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
    assertArrayEquals(new String[]{"exit"}, DpchClientShell.shellArgs("quit"));
  }

  @Test
  void helpFlagsMapToHelp() {
    assertArrayEquals(new String[]{"help"}, DpchClientShell.shellArgs("help"));
    assertArrayEquals(new String[]{"help", "depcoin-transfer"}, DpchClientShell.shellArgs("help depcoin-transfer"));
    assertArrayEquals(new String[]{"help"}, DpchClientShell.shellArgs("?"));
    assertArrayEquals(new String[]{"--help"}, DpchClientShell.shellArgs("--help"));
  }

  @Test
  void aliasesNormalizeToExpectedCommands() {
    assertArrayEquals(new String[]{"depcoin-transfer", "abc", "10", "0"}, DpchClientShell.shellArgs("t abc 10 0"));
    assertArrayEquals(new String[]{"depcoin-transfer", "abc", "10", "0"}, DpchClientShell.shellArgs("transfer abc 10 0"));
    assertArrayEquals(new String[]{"depcoin-transfer", "abc", "10", "0"}, DpchClientShell.shellArgs("dep-transfer abc 10 0"));
    assertArrayEquals(new String[]{"depcoin-transfer", "abc", "10", "0"}, DpchClientShell.shellArgs("depcoin-transfer abc 10 0"));
    assertArrayEquals(new String[]{"depcoin-balance", "abc"}, DpchClientShell.shellArgs("b abc"));
    assertArrayEquals(new String[]{"depcoin-balance", "abc"}, DpchClientShell.shellArgs("dep-balance abc"));
    assertArrayEquals(new String[]{"ist-balance", "abc"}, DpchClientShell.shellArgs("ist-balance abc"));
    assertArrayEquals(new String[]{"ist-balance", "abc"}, DpchClientShell.shellArgs("ib abc"));
    assertArrayEquals(new String[]{"ist-transfer", "abc", "15", "5"}, DpchClientShell.shellArgs("ist-transfer abc 15 5"));
    assertArrayEquals(new String[]{"ist-transfer", "abc", "15", "5"}, DpchClientShell.shellArgs("it abc 15 5"));
    assertArrayEquals(new String[]{"exit"}, DpchClientShell.shellArgs("q"));
  }
}
