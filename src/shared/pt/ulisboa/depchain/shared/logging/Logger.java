package pt.ulisboa.depchain.shared.logging;

public class Logger {
  private static final String RESET = "\u001B[0m";
  private static final String RED = "\u001B[31m";
  private static final String GREEN = "\u001B[32m";
  private static final String YELLOW = "\u001B[33m";

  public final String name;

  public Logger(String name) {
    this.name = name;
  }

  public void info(String message) {
    System.out.println(GREEN + "[INFO] [" + name + "] " + message + RESET);
  }

  public void print(String message) {
    System.out.println(message);
  }

  public void error(String message) {
    System.err.println(RED + "[ERROR] [" + name + "] " + message + RESET);
  }

  public void warn(String message) {
    System.out.println(YELLOW + "[WARN] [" + name + "] " + message + RESET);
  }

  public void debug(String message) {
    System.out.println(YELLOW + "[DEBUG] [" + name + "] " + message + RESET);
  }
}
