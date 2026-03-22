package pt.ulisboa.depchain.shared.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;

class ConfigParserTest {

  @Test
  void loadDerivesReplicaBlocksDirectoryFromConfiguredStorageRoot() throws Exception {
    ConfigParser config = ConfigParser.load(Path.of("config", "config.yaml"));

    assertEquals(Path.of("runtime", "storage"), config.storage().blocksRootPath());
    assertEquals(Path.of("runtime", "storage", "server1", "blocks"), config.blocksDirectoryForReplica("server1"));
    assertEquals(Path.of("runtime", "keys"), config.keys().rootPath());
    assertEquals(Path.of("runtime", "keys", "server1", "public", "replica.pem"), config.requireReplicaById("server1").publicKeyPath());
    assertEquals(Path.of("runtime", "keys", "server1", "private", "threshold.share"), config.requireReplicaById("server1").thresholdPrivateSharePath());
    assertEquals(Path.of("runtime", "keys", "client", "private", "client.pem"), config.client().privateKeyPath());
  }
}
