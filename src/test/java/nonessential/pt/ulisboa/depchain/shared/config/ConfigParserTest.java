package pt.ulisboa.depchain.shared.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

class ConfigParserTest {

  @Test
  void loadDerivesReplicaBlocksDirectoryFromConfiguredStorageRoot() throws Exception {
    ConfigParser config = ConfigParser.load(Path.of("config", "config.yaml"));

    assertEquals(3, config.clients().size());
    assertEquals("client", config.client().id());
    assertEquals(Path.of("runtime", "storage"), config.storage().blocksRootPath());
    assertEquals(Path.of("runtime", "storage", "server1", "blocks"), config.blocksDirectoryForReplica("server1"));
    assertEquals(Path.of("runtime", "keys"), config.keys().rootPath());
    assertEquals(Path.of("runtime", "keys", "server1", "public", "replica.pem"), config.requireReplicaById("server1").publicKeyPath());
    assertEquals(Path.of("runtime", "keys", "server1", "private", "threshold.share"), config.requireReplicaById("server1").thresholdPrivateSharePath());
    assertEquals(Path.of("runtime", "keys", "client", "private", "client.pem"), config.client().privateKeyPath());
    assertEquals(Path.of("runtime", "keys", "client2", "private", "client.pem"), config.requireClientById("client2").privateKeyPath());
    assertEquals(Path.of("runtime", "keys", "client3", "public", "client.pem"), config.requireClientById("client3").publicKeyPath());
  }

  @Test
  void loadSupportsMultipleClientsDeclaredUnderClientsSection() throws Exception {
    Path configPath = Files.createTempFile("depchain-config", ".yaml");
    Files.writeString(configPath, """
        system:
          n: 4
          f: 1

        replicas:
          server1:
            senderId: 0
            host: 127.0.0.1
            ports:
              consensus: 10001
              client: 11001
          server2:
            senderId: 1
            host: 127.0.0.1
            ports:
              consensus: 10002
              client: 11002
          server3:
            senderId: 2
            host: 127.0.0.1
            ports:
              consensus: 10003
              client: 11003
          server4:
            senderId: 3
            host: 127.0.0.1
            ports:
              consensus: 10004
              client: 11004

        clients:
          clientA:
            senderId: 100
            host: 127.0.0.1
            requestTimeoutMs: 0
            knownReplicas: [server1, server2, server3, server4]
          clientB:
            senderId: 101
            host: 127.0.0.1
            requestTimeoutMs: 250
            knownReplicas: [server1, server2]

        timeouts:
          viewChangeMs: 1000
          clientCommandWaitMs: 800
          thresholdRoundMs: 800
          fetchNodeMs: 300

        keys:
          root: runtime/keys

        storage:
          blocksRoot: runtime/storage
        """);

    ConfigParser config = ConfigParser.load(configPath);

    assertEquals(2, config.clients().size());
    assertEquals("clientA", config.client().id());
    assertEquals(101L, config.requireClientById("clientB").senderId());
    assertEquals("clientB", config.requireClientBySenderId(101L).id());
    assertEquals(Path.of("runtime", "keys", "clientB", "private", "client.pem"), config.requireClientById("clientB").privateKeyPath());
  }
}
