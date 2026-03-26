package pt.ulisboa.depchain.server.node;

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
import java.util.regex.Pattern;

import org.eclipse.jdt.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.config.GenesisParser;
import pt.ulisboa.depchain.shared.validation.ValidationUtils;

public final class BlockStore {
  private static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
  private static final String BLOCK_FILE_PREFIX = "block-";
  private static final String BLOCK_FILE_SUFFIX = ".json";
  private static final Pattern BLOCK_FILE_PATTERN = Pattern.compile("^block-\\d{8}\\.json$");

  private final Path blocksDirectory;

  public BlockStore(Path blocksDirectory) {
    this.blocksDirectory = ValidationUtils.requireNonNull(blocksDirectory, "blocksDirectory");
  }

  public static BlockStore forReplica(ConfigParser config, String replicaId) {
    ValidationUtils.requireNonNull(config, "config");
    ValidationUtils.requireNonBlank(replicaId, "replicaId");
    return new BlockStore(config.blocksDirectoryForReplica(replicaId));
  }

  public static BlockStore forReplica(ConfigParser config, ConfigParser.ReplicaSection replica) {
    ValidationUtils.requireNonNull(config, "config");
    ValidationUtils.requireNonNull(replica, "replica");
    return new BlockStore(config.blocksDirectoryForReplica(replica));
  }

  public BlockDocument ensureGenesisPersisted(BlockDocument genesisBlock) throws IOException {
    ValidationUtils.requireNonNull(genesisBlock, "genesisBlock");
    Files.createDirectories(blocksDirectory);

    Path genesisBlockPath = blockFilePath(0L);
    if (Files.exists(genesisBlockPath)) {
      try {
        return readBlock(genesisBlockPath);
      } catch (IOException exception) {
      }
    }

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
      latestPath = paths.filter(BlockStore::isBlockFile).max(Comparator.comparingLong(BlockStore::parseHeightFromPath));
    }

    if (latestPath.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(readBlock(latestPath.get()));
  }

  public Optional<BlockDocument> load(long height) throws IOException {
    ValidationUtils.requireNonNegativeLong(height, "height");
    Files.createDirectories(blocksDirectory);

    Path blockPath = blockFilePath(height);
    if (!Files.exists(blockPath)) {
      return Optional.empty();
    }

    return Optional.of(readBlock(blockPath));
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

  private static boolean isBlockFile(Path path) {
    String fileName = path.getFileName().toString();
    return BLOCK_FILE_PATTERN.matcher(fileName).matches();
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
  }
}
