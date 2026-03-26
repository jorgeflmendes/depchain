package pt.ulisboa.depchain.server.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pt.ulisboa.depchain.shared.config.GenesisParser;

class BlockStoreTest {

  @Test
  void appendRequiresSequentialHeight(@TempDir Path tempDir) throws IOException {
    BlockStore store = new BlockStore(tempDir);

    store.append(block(0L, "a", null));

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> store.append(block(2L, "c", "a")));
    assertTrue(exception.getMessage().contains("expected next block height"));
  }

  @Test
  void appendRequiresPreviousHashToMatchLatest(@TempDir Path tempDir) throws IOException {
    BlockStore store = new BlockStore(tempDir);

    store.append(block(0L, "a", null));

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> store.append(block(1L, "b", "wrong")));
    assertTrue(exception.getMessage().contains("previous_block_hash"));
  }

  @Test
  void appendAcceptsSequentiallyLinkedBlocks(@TempDir Path tempDir) throws IOException {
    BlockStore store = new BlockStore(tempDir);

    BlockStore.BlockDocument genesis = block(0L, "hash-0", null);
    BlockStore.BlockDocument next = block(1L, "hash-1", "hash-0");

    store.append(genesis);
    store.append(next);

    var latest = store.loadLatest();
    assertTrue(latest.isPresent());
    assertEquals(1L, latest.get().height());
    assertEquals("hash-1", latest.get().blockHash());
  }

  @Test
  void appendAndLoadPreservesAccountStorage(@TempDir Path tempDir) throws IOException {
    BlockStore store = new BlockStore(tempDir);

    LinkedHashMap<String, String> storage = new LinkedHashMap<>();
    storage.put("0x01", "0x02");
    LinkedHashMap<String, GenesisParser.GenesisAccount> state = new LinkedHashMap<>();
    state.put("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", new GenesisParser.GenesisAccount("100", 1L, "0x6000", storage));

    BlockStore.BlockDocument block = new BlockStore.BlockDocument(0L, "hash-0", null, 21_000L, List.of(), state);
    store.append(block);

    var latest = store.loadLatest();
    assertTrue(latest.isPresent());
    assertEquals("0x02", latest.get().state().get("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").storage().get("0x01"));
  }

  @Test
  void loadLatestIgnoresTemporaryArtifacts(@TempDir Path tempDir) throws IOException {
    BlockStore store = new BlockStore(tempDir);

    store.append(block(0L, "hash-0", null));
    Files.writeString(tempDir.resolve("block-00000001.json.tmp-123.json"), "{}");

    var latest = store.loadLatest();
    assertTrue(latest.isPresent());
    assertEquals(0L, latest.get().height());
  }

  @Test
  void appendAndLoadPreservesStateChangingTransactionTypes(@TempDir Path tempDir) throws IOException {
    BlockStore store = new BlockStore(tempDir);

    BlockStore.BlockDocument block = new BlockStore.BlockDocument(0L, "hash-0", null, 21_000L,
        List.of(new GenesisParser.GenesisTransaction("TRANSFER", "DepCoin", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "0", 0L,
            250_000L, 1L, "0x", null), new GenesisParser.GenesisTransaction("IST_COIN_TRANSFER", "IST", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "25", 1L, 250_000L, 1L, "0x", null), new GenesisParser.GenesisTransaction("CONTRACT_CALL", "N/A",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "cccccccccccccccccccccccccccccccccccccccc", "0", 2L, 250_000L, 1L, "0xdeadbeef", "0x01")),
        new LinkedHashMap<>());

    store.append(block);

    var latest = store.loadLatest();
    assertTrue(latest.isPresent());
    assertEquals("TRANSFER", latest.get().transactions().get(0).type());
    assertEquals("DepCoin", latest.get().transactions().get(0).currency());
    assertEquals("IST_COIN_TRANSFER", latest.get().transactions().get(1).type());
    assertEquals("IST", latest.get().transactions().get(1).currency());
    assertEquals("CONTRACT_CALL", latest.get().transactions().get(2).type());
    assertEquals("N/A", latest.get().transactions().get(2).currency());
    assertEquals("0xdeadbeef", latest.get().transactions().get(2).input());
  }

  private static BlockStore.BlockDocument block(long height, String hash, String previousHash) {
    return new BlockStore.BlockDocument(height, hash, previousHash, 0L, List.of(), new LinkedHashMap<>());
  }
}
