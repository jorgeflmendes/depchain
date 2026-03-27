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

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> store.append(block(2L, "c", hash("a"))));
    assertTrue(exception.getMessage().contains("expected next block height"));
  }

  @Test
  void appendRequiresPreviousHashToMatchLatest(@TempDir Path tempDir) throws IOException {
    BlockStore store = new BlockStore(tempDir);

    store.append(block(0L, "a", null));

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> store.append(block(1L, "b", hash("wrong"))));
    assertTrue(exception.getMessage().contains("previous_block_hash"));
  }

  @Test
  void appendAcceptsSequentiallyLinkedBlocks(@TempDir Path tempDir) throws IOException {
    BlockStore store = new BlockStore(tempDir);

    BlockStore.BlockDocument genesis = block(0L, "hash-0", null);
    BlockStore.BlockDocument next = block(1L, "hash-1", genesis.blockHash());

    store.append(genesis);
    store.append(next);

    var latest = store.loadLatest();
    assertTrue(latest.isPresent());
    assertEquals(1L, latest.get().height());
    assertEquals(BlockStore.computeBlockHash(genesis.blockHash(), 0L, List.of()), latest.get().blockHash());
  }

  @Test
  void appendAndLoadPreservesAccountStorage(@TempDir Path tempDir) throws IOException {
    BlockStore store = new BlockStore(tempDir);

    LinkedHashMap<String, String> storage = new LinkedHashMap<>();
    storage.put("0x01", "0x02");
    LinkedHashMap<String, GenesisParser.GenesisAccount> state = new LinkedHashMap<>();
    state.put("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", new GenesisParser.GenesisAccount("100", 1L, "0x6000", storage));

    BlockStore.BlockDocument block = new BlockStore.BlockDocument(0L, hash("hash-0"), null, 0L, List.of(), state);
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

    BlockStore.BlockDocument block = new BlockStore.BlockDocument(0L, hash("hash-0"), null, 21_000L,
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

  @Test
  void appendRejectsMismatchedDerivedBlockHash(@TempDir Path tempDir) throws IOException {
    BlockStore store = new BlockStore(tempDir);
    BlockStore.BlockDocument genesis = block(0L, "hash-0", null);
    store.append(genesis);

    LinkedHashMap<String, GenesisParser.GenesisAccount> state = new LinkedHashMap<>();
    List<GenesisParser.GenesisTransaction> transactions = List.of(transaction("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 0L, 21_000L));
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> store
        .append(new BlockStore.BlockDocument(1L, hash("wrong"), genesis.blockHash(), 21_000L, transactions, state)));

    assertTrue(exception.getMessage().contains("block_hash"));
  }

  @Test
  void appendRejectsNonConsecutiveSenderNoncesInsideBlock(@TempDir Path tempDir) throws IOException {
    BlockStore store = new BlockStore(tempDir);
    BlockStore.BlockDocument genesis = block(0L, "hash-0", null);
    store.append(genesis);

    List<GenesisParser.GenesisTransaction> transactions = List
        .of(transaction("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 0L, 21_000L), transaction("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 2L, 21_000L));
    String blockHash = BlockStore.computeBlockHash(genesis.blockHash(), 42_000L, transactions);

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> store
        .append(new BlockStore.BlockDocument(1L, blockHash, genesis.blockHash(), 42_000L, transactions, new LinkedHashMap<>())));

    assertTrue(exception.getMessage().contains("consecutive nonces"));
  }

  @Test
  void appendRejectsTransactionGasLimitSumAboveBlockLimit(@TempDir Path tempDir) throws IOException {
    BlockStore store = new BlockStore(tempDir);
    BlockStore.BlockDocument genesis = block(0L, "hash-0", null);
    store.append(genesis);

    List<GenesisParser.GenesisTransaction> transactions = List
        .of(transaction("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 0L, BlockStore.MAX_BLOCK_GAS_LIMIT), transaction("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 0L, 1L));
    String blockHash = BlockStore.computeBlockHash(genesis.blockHash(), BlockStore.MAX_BLOCK_GAS_LIMIT, transactions);

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> store
        .append(new BlockStore.BlockDocument(1L, blockHash, genesis.blockHash(), BlockStore.MAX_BLOCK_GAS_LIMIT, transactions, new LinkedHashMap<>())));

    assertTrue(exception.getMessage().contains("block gas limit"));
  }

  private static BlockStore.BlockDocument block(long height, String hash, String previousHash) {
    if (height == 0L) {
      return new BlockStore.BlockDocument(0L, hash(hash), null, 0L, List.of(), new LinkedHashMap<>());
    }
    return new BlockStore.BlockDocument(height, BlockStore.computeBlockHash(previousHash, 0L, List.of()), previousHash, 0L, List.of(), new LinkedHashMap<>());
  }

  private static GenesisParser.GenesisTransaction transaction(String from, long nonce, long gasLimit) {
    return new GenesisParser.GenesisTransaction("TRANSFER", "DepCoin", from, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "1", nonce, gasLimit, 1L, "0x", null);
  }

  private static String hash(String value) {
    return pt.ulisboa.depchain.shared.crypto.CryptoUtil.sha256Hex(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }
}
