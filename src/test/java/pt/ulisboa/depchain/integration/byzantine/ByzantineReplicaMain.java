package pt.ulisboa.depchain.integration.byzantine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ByzantineReplicaMain {
  private static final Logger logger = LoggerFactory.getLogger(ByzantineReplicaMain.class);

  private ByzantineReplicaMain() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      logger.error("Usage: ByzantineReplicaMain <serverId> <configPath> <attackMode>");
      System.exit(1);
    }

    ByzantineReplicaServer server = new ByzantineReplicaServer(args[0], args[1], ByzantineAttackMode.valueOf(args[2]));
    server.run();
  }
}
