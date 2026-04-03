package pt.ulisboa.depchain.integration.support;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import pt.ulisboa.depchain.shared.config.ConfigParser;

class IntegrationHarnessConfigIsolationTest extends IntegrationHarness {
  @Test
  void isolatedIntegrationConfigDoesNotInheritProjectGenesisLock() throws Exception {
    Path configPath = integrationConfigPath();
    Path configDirectory = configPath.toAbsolutePath().normalize().getParent();

    try {
      assertTrue(Files.exists(configDirectory.resolve("genesis.json")));
      assertFalse(Files.exists(configDirectory.resolve("genesis.lock.json")));

      populateConfig(configPath);

      ConfigParser config = ConfigParser.load(configPath);

      assertTrue(Files.exists(configDirectory.resolve("addresses.json")));
      assertFalse(Files.exists(configDirectory.resolve("genesis.lock.json")));
      assertEquals(0, config.client().requestTimeoutMs());
      assertEquals(4_000, config.timeouts().viewChangeMs());
      assertEquals(2_500, config.timeouts().clientCommandWaitMs());
      assertEquals(2_500, config.timeouts().thresholdRoundMs());
      assertEquals(1_500, config.timeouts().fetchNodeMs());
    } finally {
      cleanupIsolatedConfigDirectory(configPath);
    }
  }
}
