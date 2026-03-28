package pt.ulisboa.depchain.server.node;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
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
import pt.ulisboa.depchain.shared.crypto.CryptoUtil;
import pt.ulisboa.depchain.shared.validation.ValidationUtils;

public final class BlockStore {
  public static final String FAIL_AFTER_TEMP_WRITE_HEIGHT_PROPERTY = "depchain.blockstore.failAfterTempWriteHeight";
  private static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
  private static final String BLOCK_FILE_PREFIX = "block-";
  private static final String BLOCK_FILE_SUFFIX = ".json";
  private static final Pattern BLOCK_FILE_PATTERN = Pattern.compile("^block-\\d{8}\\.json$");
  private static final Pattern SHA_256_HEX_PATTERN = Pattern.compile("^[0-9a-f]{64}$");
  public static final long MAX_BLOCK_GAS_LIMIT = 30_000_000L;

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

  public List<BlockDocument> loadAll() throws IOException {
    Files.createDirectories(blocksDirectory);

    List<Path> blockPaths;
    try (var paths = Files.list(blocksDirectory)) {
      blockPaths = paths.filter(BlockStore::isBlockFile).sorted(Comparator.comparingLong(BlockStore::parseHeightFromPath)).toList();
    }

    java.util.ArrayList<BlockDocument> blocks = new java.util.ArrayList<>(blockPaths.size());
    for (Path blockPath : blockPaths) {
      blocks.add(readBlock(blockPath));
    }
    return List.copyOf(blocks);
  }

  public static String computeBlockHash(@Nullable String previousBlockHash, long gasUsed, List<GenesisParser.GenesisTransaction> transactions) {
    ValidationUtils.requireAllNonNull(ValidationUtils.named("transactions", transactions));
    ValidationUtils.requireNonNegativeLong(gasUsed, "gas_used");

    StringBuilder payload = new StringBuilder(previousBlockHash == null ? "GENESIS" : previousBlockHash);
    payload.append("|gas_used=").append(gasUsed);
    for (GenesisParser.GenesisTransaction transaction : transactions) {
      ValidationUtils.requireNonNull(transaction, "transaction");
      payload.append("|tx{type=").append(transaction.type()).append(",currency=").append(transaction.currency()).append(",from=").append(transaction.from()).append(",to=")
          .append(transaction.to() == null ? "" : transaction.to()).append(",amount=").append(transaction.amount()).append(",nonce=").append(transaction.nonce())
          .append(",gas_limit=").append(transaction.gasLimit()).append(",gas_price=").append(transaction.gasPrice()).append(",input=").append(transaction.input())
          .append(",signature=").append(transaction.signature() == null ? "" : transaction.signature()).append('}');
    }
    return CryptoUtil.sha256Hex(payload.toString().getBytes(StandardCharsets.UTF_8));
  }

  private void writeBlock(BlockDocument block) throws IOException {
    Path blockPath = blockFilePath(block.height());
    Path temporaryPath = Files.createTempFile(blocksDirectory, blockPath.getFileName() + ".tmp-", ".json");
    try (OutputStream output = Files.newOutputStream(temporaryPath)) {
      JSON.writeValue(output, block);
    }
    if (shouldFailAfterTempWrite(block.height())) {
      throw new IOException("Simulated block persistence failure after temporary write");
    }
    Files.move(temporaryPath, blockPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
  }

  private static boolean shouldFailAfterTempWrite(long height) {
    String configuredHeight = System.getProperty(FAIL_AFTER_TEMP_WRITE_HEIGHT_PROPERTY);
    if (configuredHeight == null || configuredHeight.isBlank()) {
      return false;
    }

    try {
      return Long.parseLong(configuredHeight) == height;
    } catch (NumberFormatException ignored) {
      return false;
    }
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
      blockHash = requireSha256Hash(blockHash, "block_hash");
      ValidationUtils.requireAllNonNull(ValidationUtils.named("transactions", transactions), ValidationUtils.named("state", state));
      ValidationUtils.requireNonNegativeLong(gasUsed, "gas_used");
      transactions = List.copyOf(transactions);
      validateTransactionLayout(height, previousBlockHash, gasUsed, transactions, blockHash);
      state = new LinkedHashMap<>(state);
    }

    private static void validateTransactionLayout(long height, @Nullable String previousBlockHash, long gasUsed, List<GenesisParser.GenesisTransaction> transactions, String blockHash) {
      if (height == 0L) {
        if (previousBlockHash != null) {
          throw new IllegalArgumentException("genesis persisted block must not set previous_block_hash");
        }
      } else {
        String normalizedPreviousHash = requireSha256Hash(previousBlockHash, "previous_block_hash");
        String expectedHash = computeBlockHash(normalizedPreviousHash, gasUsed, transactions);
        if (!blockHash.equals(expectedHash)) {
          throw new IllegalArgumentException("block_hash must match the hash of previous_block_hash plus the block transactions");
        }
      }

      long totalGasLimit = 0L;
      LinkedHashMap<String, Long> latestNonceBySender = new LinkedHashMap<>();
      for (GenesisParser.GenesisTransaction transaction : transactions) {
        totalGasLimit = Math.addExact(totalGasLimit, transaction.gasLimit());
        if (totalGasLimit > MAX_BLOCK_GAS_LIMIT) {
          throw new IllegalArgumentException("sum of transaction gas_limit values must not exceed the block gas limit");
        }

        Long previousNonce = latestNonceBySender.put(transaction.from(), transaction.nonce());
        if (previousNonce != null && transaction.nonce() != previousNonce + 1L) {
          throw new IllegalArgumentException("transactions from the same sender must use consecutive nonces inside a block");
        }
      }

      if (gasUsed > MAX_BLOCK_GAS_LIMIT) {
        throw new IllegalArgumentException("gas_used must not exceed the block gas limit");
      }
      if (!transactions.isEmpty() && gasUsed > totalGasLimit) {
        throw new IllegalArgumentException("gas_used must not exceed the sum of transaction gas_limit values");
      }
      if (transactions.isEmpty() && gasUsed != 0L) {
        throw new IllegalArgumentException("blocks without transactions must use gas_used = 0");
      }
    }
  }

  private static String requireSha256Hash(@Nullable String value, String fieldName) {
    String nonBlankValue = ValidationUtils.requireNonBlank(value, fieldName);
    if (!SHA_256_HEX_PATTERN.matcher(nonBlankValue).matches()) {
      throw new IllegalArgumentException(fieldName + " must be a 64-char lowercase SHA-256 hex string");
    }
    return nonBlankValue;
  }
}
