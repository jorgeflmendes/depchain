package pt.ulisboa.depchain.shared.keys;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;
import pt.ulisboa.depchain.testsupport.TestKeyMaterialSupport;

class KeyLoaderTest {
  @BeforeAll
  static void ensureKeyMaterial() throws Exception {
    TestKeyMaterialSupport.ensureKeyMaterial(configPath());
  }

  @Test
  void loadsReplicaAndClientPemKeysFromConfig() throws Exception {
    ConfigParser config = ConfigParser.load(configPath());

    Map<Long, PublicKey> replicaKeys = PublicKeyLoader.loadReplicaPublicKeys(config);
    Map<Long, PublicKey> staticKeys = PublicKeyLoader.loadStaticPublicKeys(config);
    PrivateKey replicaPrivateKey = PrivateKeyLoader.loadReplicaPrivateKey(config, 0L);
    PrivateKey clientPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config);

    assertEquals(config.system().n(), replicaKeys.size());
    assertEquals(config.system().n() + 1, staticKeys.size());
    assertNotNull(replicaPrivateKey);
    assertNotNull(clientPrivateKey);
    assertTrue(staticKeys.containsKey(config.client().senderId()));
  }

  @Test
  void decodePrivateKeyRejectsNonPemBytes() throws Exception {
    KeyPair keyPair = CryptoUtil.newECKeyPair();

    assertThrows(IllegalArgumentException.class, () -> PrivateKeyLoader.decodePrivateKey(keyPair.getPrivate().getEncoded()));
  }

  @Test
  void decodePublicKeyAcceptsX509BytesForEphemeralHandshakeKeys() throws Exception {
    KeyPair keyPair = CryptoUtil.newECKeyPair();

    PublicKey decoded = PublicKeyLoader.decodePublicKey(keyPair.getPublic().getEncoded());

    assertNotNull(decoded);
    assertEquals("EC", decoded.getAlgorithm());
  }

  private static Path configPath() {
    return Path.of(System.getProperty("user.dir"), "config", "config.yaml").toAbsolutePath();
  }
}
