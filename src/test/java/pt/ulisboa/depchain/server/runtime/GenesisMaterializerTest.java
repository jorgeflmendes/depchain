package pt.ulisboa.depchain.server.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.security.PublicKey;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.config.GenesisParser;
import pt.ulisboa.depchain.shared.keys.PublicKeyLoader;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;
import pt.ulisboa.depchain.testsupport.TestKeyMaterialSupport;

class GenesisMaterializerTest {
  @Test
  void materializedGenesisFundsConfiguredClients() throws Exception {
    Path configPath = Path.of(System.getProperty("user.dir"), "config", "config.yaml");
    TestKeyMaterialSupport.ensureKeyMaterial(configPath);

    ConfigParser config = ConfigParser.load(configPath);
    Map<Long, PublicKey> clientPublicKeys = PublicKeyLoader.loadClientPublicKeys(config);
    GenesisParser template = GenesisParser.loadForConfig(configPath);
    GenesisParser materialized = GenesisMaterializer.materialize(template, config, clientPublicKeys);

    String clientAddress = CryptoUtil.deriveAddressHex(clientPublicKeys.get(config.client().senderId()));
    String client2Address = CryptoUtil.deriveAddressHex(clientPublicKeys.get(config.requireClientById("client2").senderId()));
    List<GenesisParser.GenesisTransaction> depcoinTransfers = materialized.transactions().stream().filter(transaction -> "TRANSFER".equals(transaction.type())).toList();

    assertTrue(materialized.state().containsKey(clientAddress));
    assertTrue(materialized.state().containsKey(client2Address));
    assertEquals(clientAddress, depcoinTransfers.getFirst().to());
    assertEquals(client2Address, depcoinTransfers.get(1).to());
    assertEquals(clientAddress, decodeTransferRecipient(materialized.transactions().get(1).input()));
  }

  private static String decodeTransferRecipient(String input) {
    String hex = input.substring(2);
    return hex.substring(8 + 24, 8 + 64);
  }
}
