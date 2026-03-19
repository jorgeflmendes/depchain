package pt.ulisboa.depchain.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.junit.jupiter.api.Test;

class DpchClientShellArgsTest {
  @Test
  void bareInputFallsBackToAppendCommand() {
    assertArrayEquals(new String[]{"append", "hello world"}, DpchClientShell.shellArgs("hello world"));
  }

  @Test
  void appendCommandKeepsRemainderAsSingleValue() {
    assertArrayEquals(new String[]{"append", "hello world"}, DpchClientShell.shellArgs("append hello world"));
  }

  @Test
  void exitAliasesNormalizeToExit() {
    assertArrayEquals(new String[]{"exit"}, DpchClientShell.shellArgs("EXIT"));
    assertArrayEquals(new String[]{"exit"}, DpchClientShell.shellArgs("quit"));
  }

  @Test
  void helpFlagsMapToHelp() {
    assertArrayEquals(new String[]{"help"}, DpchClientShell.shellArgs("help"));
    assertArrayEquals(new String[]{"help", "append"}, DpchClientShell.shellArgs("help append"));
    assertArrayEquals(new String[]{"help"}, DpchClientShell.shellArgs("?"));
    assertArrayEquals(new String[]{"--help"}, DpchClientShell.shellArgs("--help"));
  }

  @Test
  void aliasesNormalizeToExpectedCommands() {
    assertArrayEquals(new String[]{"append", "hello"}, DpchClientShell.shellArgs("a hello"));
    assertArrayEquals(new String[]{"exit"}, DpchClientShell.shellArgs("q"));
  }
}
