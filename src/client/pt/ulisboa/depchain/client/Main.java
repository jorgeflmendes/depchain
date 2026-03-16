package pt.ulisboa.depchain.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Main {
  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      logger.error("Usage: Main <configPath>");
      System.exit(1);
    }

    String configPath = args[0];

    DpchClient client = new DpchClient(configPath);
    client.run();
  }
}
