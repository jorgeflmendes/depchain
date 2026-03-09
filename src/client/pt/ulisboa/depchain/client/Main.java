package pt.ulisboa.depchain.client;

import java.util.Scanner;

public final class Main {
  public static final String RESET = "\u001B[0m";
  public static final String RED = "\u001B[31m";
  public static final String GREEN = "\u001B[32m";
  public static final String YELLOW = "\u001B[33m";

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: Main <targetReplicaId> <configPath>");
      System.exit(1);
    }

    String targetReplicaId = args[0];
    String configPath = args[1];

    DpchClient client = new DpchClient(configPath);

    Scanner scanner = new Scanner(System.in);
    while (true) {
      System.out.println(YELLOW + "Enter a value to append or 'EXIT' to quit:" + RESET);
      String input = scanner.nextLine();

      if (input.equalsIgnoreCase("EXIT")) {
        break;
      } else {
        try {
          String response = client.append(input, targetReplicaId);
          System.out.println(GREEN + "response = " + response + RESET);
        } catch (Exception e) {
          System.err.println(RED + "Error appending value through the server " + targetReplicaId + ": " + e.getMessage() + RESET);
        }

      }
    }

    scanner.close();
    System.out.println(GREEN + "Shutting down..." + RESET);
  }
}
