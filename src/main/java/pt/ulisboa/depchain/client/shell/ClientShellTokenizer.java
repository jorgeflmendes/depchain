package pt.ulisboa.depchain.client.shell;

final class ClientShellTokenizer {
  private ClientShellTokenizer() {
  }

  static String[] shellArgs(String input) {
    String trimmedInput = input == null ? "" : input.trim();
    if (trimmedInput.isEmpty()) {
      return new String[0];
    }

    int firstWhitespace = firstWhitespaceIndex(trimmedInput);
    String commandToken = firstWhitespace >= 0 ? trimmedInput.substring(0, firstWhitespace) : trimmedInput;
    String normalizedCommand = commandToken.toLowerCase();

    return switch (normalizedCommand) {
      case "depcoin-transfer" -> tokenizeCommand(trimmedInput, firstWhitespace, "depcoin-transfer");
      case "ist-transfer" -> tokenizeCommand(trimmedInput, firstWhitespace, "ist-transfer");
      case "contract-call" -> tokenizeCommand(trimmedInput, firstWhitespace, "contract-call");
      case "depcoin-balance" -> tokenizeCommand(trimmedInput, firstWhitespace, "depcoin-balance");
      case "ist-balance" -> tokenizeCommand(trimmedInput, firstWhitespace, "ist-balance");
      case "my-address" -> new String[]{"my-address"};
      case "exit" -> new String[]{"exit"};
      case "help" -> tokenizeCommand(trimmedInput, firstWhitespace, "help");
      case "-h", "--help" -> new String[]{"--help"};
      default -> tokenizeCommand(trimmedInput, firstWhitespace, normalizedCommand);
    };
  }

  private static int firstWhitespaceIndex(String input) {
    for (int i = 0; i < input.length(); i++) {
      if (Character.isWhitespace(input.charAt(i))) {
        return i;
      }
    }
    return -1;
  }

  private static String[] tokenizeCommand(String trimmedInput, int firstWhitespace, String normalizedCommand) {
    if (firstWhitespace < 0) {
      return new String[]{normalizedCommand};
    }

    String remainder = trimmedInput.substring(firstWhitespace).trim();
    if (remainder.isEmpty()) {
      return new String[]{normalizedCommand};
    }

    String[] args = remainder.split("\\s+");
    String[] command = new String[args.length + 1];
    command[0] = normalizedCommand;
    System.arraycopy(args, 0, command, 1, args.length);
    return command;
  }
}
