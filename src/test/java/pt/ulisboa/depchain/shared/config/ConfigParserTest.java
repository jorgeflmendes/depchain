package pt.ulisboa.depchain.shared.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConfigParserTest {
  @TempDir
  Path tempDir;

  @Test
  void loadRejectsReplicaWithoutPublicKeyPath() throws Exception {
    Path configPath = tempDir.resolve("config.properties");
    Files.writeString(configPath, configWithoutReplicaPublicKeyPath(), StandardCharsets.UTF_8);

    IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> ConfigParser.load(configPath));

    assertTrue(error.getMessage().contains("replica.server1.publicKeyPath"));
  }

  @Test
  void loadParsesValidConfiguration() throws Exception {
    Path configPath = tempDir.resolve("config.properties");
    Files.writeString(configPath, validConfig(), StandardCharsets.UTF_8);

    ConfigParser config = ConfigParser.load(configPath);

    assertEquals(4, config.system().n());
    assertEquals(4, config.replicas().size());
    assertEquals("server1", config.replicas().getFirst().id());
    assertEquals(1500, config.timeouts().viewChangeMs());
    assertEquals(250, config.timeouts().retransmitMs());
    assertEquals(10_000, config.timeouts().maxBackoffMs());
  }

  @Test
  void loadRejectsUnknownKnownReplica() throws Exception {
    Path configPath = tempDir.resolve("config.properties");
    Files.writeString(configPath, configWithUnknownKnownReplica(), StandardCharsets.UTF_8);

    IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> ConfigParser.load(configPath));

    assertTrue(error.getMessage().contains("client.knownReplicas contains unknown replica"));
  }

  private static String configWithoutReplicaPublicKeyPath() {
    return """
        system.n=4
        system.f=1

        replicas=server1,server2,server3,server4

        replica.server1.senderId=1
        replica.server1.host=127.0.0.1
        replica.server1.consensusPort=10001
        replica.server1.clientPort=11001
        replica.server1.privateKeyPath=config/keys/server1/private/replica.pem
        replica.server1.thresholdPublicKeyPath=config/keys/server1/public/threshold.pub
        replica.server1.thresholdPrivateSharePath=config/keys/server1/private/threshold.share

        replica.server2.senderId=2
        replica.server2.host=127.0.0.1
        replica.server2.consensusPort=10002
        replica.server2.clientPort=11002
        replica.server2.publicKeyPath=config/keys/server2/public/replica.pem
        replica.server2.privateKeyPath=config/keys/server2/private/replica.pem
        replica.server2.thresholdPublicKeyPath=config/keys/server2/public/threshold.pub
        replica.server2.thresholdPrivateSharePath=config/keys/server2/private/threshold.share

        replica.server3.senderId=3
        replica.server3.host=127.0.0.1
        replica.server3.consensusPort=10003
        replica.server3.clientPort=11003
        replica.server3.publicKeyPath=config/keys/server3/public/replica.pem
        replica.server3.privateKeyPath=config/keys/server3/private/replica.pem
        replica.server3.thresholdPublicKeyPath=config/keys/server3/public/threshold.pub
        replica.server3.thresholdPrivateSharePath=config/keys/server3/private/threshold.share

        replica.server4.senderId=4
        replica.server4.host=127.0.0.1
        replica.server4.consensusPort=10004
        replica.server4.clientPort=11004
        replica.server4.publicKeyPath=config/keys/server4/public/replica.pem
        replica.server4.privateKeyPath=config/keys/server4/private/replica.pem
        replica.server4.thresholdPublicKeyPath=config/keys/server4/public/threshold.pub
        replica.server4.thresholdPrivateSharePath=config/keys/server4/private/threshold.share

        client.id=client
        client.senderId=100
        client.host=127.0.0.1
        client.publicKeyPath=config/keys/client/public/client.pem
        client.privateKeyPath=config/keys/client/private/client.pem
        client.requestTimeoutMs=3000
        client.knownReplicas=server1,server2,server3,server4

        timeouts.viewChangeMs=1500
        timeouts.retransmitMs=250
        timeouts.maxBackoffMs=10000
        """;
  }

  private static String validConfig() {
    return """
        system.n=4
        system.f=1

        replicas=server1,server2,server3,server4

        replica.server1.senderId=1
        replica.server1.host=127.0.0.1
        replica.server1.consensusPort=10001
        replica.server1.clientPort=11001
        replica.server1.publicKeyPath=config/keys/server1/public/replica.pem
        replica.server1.privateKeyPath=config/keys/server1/private/replica.pem
        replica.server1.thresholdPublicKeyPath=config/keys/server1/public/threshold.pub
        replica.server1.thresholdPrivateSharePath=config/keys/server1/private/threshold.share

        replica.server2.senderId=2
        replica.server2.host=127.0.0.1
        replica.server2.consensusPort=10002
        replica.server2.clientPort=11002
        replica.server2.publicKeyPath=config/keys/server2/public/replica.pem
        replica.server2.privateKeyPath=config/keys/server2/private/replica.pem
        replica.server2.thresholdPublicKeyPath=config/keys/server2/public/threshold.pub
        replica.server2.thresholdPrivateSharePath=config/keys/server2/private/threshold.share

        replica.server3.senderId=3
        replica.server3.host=127.0.0.1
        replica.server3.consensusPort=10003
        replica.server3.clientPort=11003
        replica.server3.publicKeyPath=config/keys/server3/public/replica.pem
        replica.server3.privateKeyPath=config/keys/server3/private/replica.pem
        replica.server3.thresholdPublicKeyPath=config/keys/server3/public/threshold.pub
        replica.server3.thresholdPrivateSharePath=config/keys/server3/private/threshold.share

        replica.server4.senderId=4
        replica.server4.host=127.0.0.1
        replica.server4.consensusPort=10004
        replica.server4.clientPort=11004
        replica.server4.publicKeyPath=config/keys/server4/public/replica.pem
        replica.server4.privateKeyPath=config/keys/server4/private/replica.pem
        replica.server4.thresholdPublicKeyPath=config/keys/server4/public/threshold.pub
        replica.server4.thresholdPrivateSharePath=config/keys/server4/private/threshold.share

        client.id=client
        client.senderId=100
        client.host=127.0.0.1
        client.publicKeyPath=config/keys/client/public/client.pem
        client.privateKeyPath=config/keys/client/private/client.pem
        client.requestTimeoutMs=3000
        client.knownReplicas=server1,server2,server3,server4

        timeouts.viewChangeMs=1500
        timeouts.retransmitMs=250
        timeouts.maxBackoffMs=10000
        """;
  }

  private static String configWithUnknownKnownReplica() {
    return validConfig().replace("client.knownReplicas=server1,server2,server3,server4", "client.knownReplicas=server1,server2,server5");
  }
}
