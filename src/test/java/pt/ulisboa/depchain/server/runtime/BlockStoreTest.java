package pt.ulisboa.depchain.server.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

  private static BlockStore.BlockDocument block(long height, String hash, String previousHash) {
    return new BlockStore.BlockDocument(height, hash, previousHash, 0L, List.of(), new LinkedHashMap<>());
  }
}
