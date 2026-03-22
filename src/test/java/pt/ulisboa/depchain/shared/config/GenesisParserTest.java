package pt.ulisboa.depchain.shared.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GenesisParserTest {
  private static final String SENDER = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  private static final String RECIPIENT = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
  private static final String EMPTY_HASH = "0000000000000000000000000000000000000000000000000000000000000000";

  @Test
  void loadNormalizesParsedGenesis(@TempDir Path tempDir) throws Exception {
    Path genesisPath = tempDir.resolve("genesis.json");
    Files.writeString(genesisPath, """
        {
          // basic transfer transaction with a commented genesis fixture
          "height": 0,
          "block_hash": "%s",
          "previous_block_hash": null,
          "gas_used": 0,
          "transactions": [
            {
              "currency": "DepCoin",
              "type": "transfer",
              "from": "%s",
              "to": "%s",
              "amount": "7",
              "nonce": 0,
              "gas_limit": 21000,
              "gas_price": 1,
              "input": "0x",
              "signature": "  "
            }
          ],
          "state": {
            "%s": {
              "balance": "10",
              "nonce": 0,
              "code": null,
              "storage": {
                "0x01": "0x02"
              }
            }
          }
        }
        """.formatted(EMPTY_HASH, SENDER, RECIPIENT, SENDER));

    GenesisParser genesis = GenesisParser.load(genesisPath);

    assertEquals("TRANSFER", genesis.transactions().getFirst().type());
    assertEquals(RECIPIENT, genesis.transactions().getFirst().to());
    assertEquals("0x02", genesis.state().get(SENDER).storage().get("0x01"));
  }

  @Test
  void loadDefaultParsesGenesisFile() throws Exception {
    GenesisParser genesis = GenesisParser.loadDefault();

    assertEquals(0L, genesis.height());
    assertFalse(genesis.transactions().isEmpty());
    assertFalse(genesis.state().isEmpty());
  }

  @Test
  void loadSupportsJsonComments(@TempDir Path tempDir) throws Exception {
    Path genesisPath = tempDir.resolve("genesis.json");
    Files.writeString(genesisPath, """
        {
          "height": 0,
          "block_hash": "%s",
          "previous_block_hash": null,
          "gas_used": 0,
          "transactions": [],
          "state": {
            // client
            "%s": {
              "balance": "10",
              "nonce": 0,
              "code": null,
              "storage": {}
            }
          }
        }
        """.formatted(EMPTY_HASH, SENDER));

    GenesisParser genesis = GenesisParser.load(genesisPath);

    assertEquals("10", genesis.state().get(SENDER).balance());
  }

  @Test
  void configRelativeGenesisPathUsesSiblingFile(@TempDir Path tempDir) {
    Path configPath = tempDir.resolve("config.yaml");
    Path expectedGenesisPath = tempDir.resolve("genesis.json");

    assertEquals(expectedGenesisPath, GenesisParser.genesisPathForConfig(configPath));
  }
}
