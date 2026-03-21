package pt.ulisboa.depchain.server.runtime;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

import org.eclipse.jdt.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import pt.ulisboa.depchain.shared.config.GenesisParser;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class BlockStore {
  private static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
  private static final String BLOCK_FILE_PREFIX = "block-";
  private static final String BLOCK_FILE_SUFFIX = ".json";

  private final Path blocksDirectory;

  public BlockStore(Path blocksDirectory) {
    this.blocksDirectory = ValidationUtils.requireNonNull(blocksDirectory, "blocksDirectory");
  }

  public static BlockStore defaultStore() {
    // Keep persistence outside resources because resources are packaged artifacts.
    return new BlockStore(Path.of("data", "blocks"));
  }

  public BlockDocument ensureGenesisPersisted() throws IOException {
    Files.createDirectories(blocksDirectory);

    Path genesisBlockPath = blockFilePath(0L);
    if (Files.exists(genesisBlockPath)) {
      try {
        return readBlock(genesisBlockPath);
      } catch (IOException exception) {
        // Another replica may have been writing genesis concurrently.
        // Regenerate deterministic genesis content and overwrite atomically.
      }
    }

    GenesisParser genesis = GenesisParser.loadDefaultResource();
    BlockDocument genesisBlock = BlockDocument.fromGenesis(genesis);
    writeBlock(genesisBlock);
    return genesisBlock;
  }

  public void append(BlockDocument block) throws IOException {
    ValidationUtils.requireNonNull(block, "block");
    Files.createDirectories(blocksDirectory);

    Optional<BlockDocument> latest = loadLatest();
    if (latest.isEmpty()) {
      if (block.height() != 0L) {
        throw new IllegalArgumentException("first persisted block must have height 0");
      }
      if (block.previousBlockHash() != null) {
        throw new IllegalArgumentException("genesis persisted block must not set previous_block_hash");
      }
    } else {
      BlockDocument latestBlock = latest.get();
      long expectedHeight = latestBlock.height() + 1L;
      if (block.height() != expectedHeight) {
        throw new IllegalArgumentException("expected next block height " + expectedHeight + " but got " + block.height());
      }
      if (block.previousBlockHash() == null || !block.previousBlockHash().equals(latestBlock.blockHash())) {
        throw new IllegalArgumentException("block previous_block_hash must match latest block hash");
      }
    }

    if (Files.exists(blockFilePath(block.height()))) {
      throw new IllegalStateException("block height " + block.height() + " already exists");
    }

    writeBlock(block);
  }

  public Optional<BlockDocument> loadLatest() throws IOException {
    Files.createDirectories(blocksDirectory);

    Optional<Path> latestPath;
    try (var paths = Files.list(blocksDirectory)) {
      latestPath = paths.filter(path -> path.getFileName().toString().startsWith(BLOCK_FILE_PREFIX) && path.getFileName().toString().endsWith(BLOCK_FILE_SUFFIX))
          .max(Comparator.comparingLong(BlockStore::parseHeightFromPath));
    }

    if (latestPath.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(readBlock(latestPath.get()));
  }

  private void writeBlock(BlockDocument block) throws IOException {
    Path blockPath = blockFilePath(block.height());
    Path temporaryPath = Files.createTempFile(blocksDirectory, blockPath.getFileName() + ".tmp-", ".json");
    try (OutputStream output = Files.newOutputStream(temporaryPath)) {
      JSON.writeValue(output, block);
    }
    Files.move(temporaryPath, blockPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
  }

  private BlockDocument readBlock(Path blockPath) throws IOException {
    try (InputStream input = Files.newInputStream(blockPath)) {
      return JSON.readValue(input, BlockDocument.class);
    }
  }

  private Path blockFilePath(long height) {
    ValidationUtils.requireNonNegativeLong(height, "height");
    return blocksDirectory.resolve(BLOCK_FILE_PREFIX + String.format("%08d", height) + BLOCK_FILE_SUFFIX);
  }

  private static long parseHeightFromPath(Path path) {
    String fileName = path.getFileName().toString();
    String numericPart = fileName.substring(BLOCK_FILE_PREFIX.length(), fileName.length() - BLOCK_FILE_SUFFIX.length());
    return Long.parseLong(numericPart);
  }

  public record BlockDocument(long height, @JsonProperty("block_hash") String blockHash, @JsonProperty("previous_block_hash") @Nullable String previousBlockHash,
      @JsonProperty("gas_used") long gasUsed, List<GenesisParser.GenesisTransaction> transactions, LinkedHashMap<String, GenesisParser.GenesisAccount> state) {

    public BlockDocument {
      ValidationUtils.requireNonNegativeLong(height, "height");
      ValidationUtils.requireNonBlank(blockHash, "block_hash");
      ValidationUtils.requireAllNonNull(ValidationUtils.named("transactions", transactions), ValidationUtils.named("state", state));
      ValidationUtils.requireNonNegativeLong(gasUsed, "gas_used");
      transactions = List.copyOf(transactions);
      state = new LinkedHashMap<>(state);
    }

    public static BlockDocument fromGenesis(GenesisParser genesis) {
      return new BlockDocument(genesis.height(), genesis.blockHash(), genesis.previousBlockHash(), genesis.gasUsed(), genesis.transactions(), genesis.state());
    }
  }
}
