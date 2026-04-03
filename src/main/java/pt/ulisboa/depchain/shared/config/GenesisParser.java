package pt.ulisboa.depchain.shared.config;

import static pt.ulisboa.depchain.shared.validation.ValidationUtils.named;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import org.eclipse.jdt.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;

import pt.ulisboa.depchain.shared.model.AccountKind;
import pt.ulisboa.depchain.shared.validation.ValidationUtils;

public record GenesisParser(long height, @JsonProperty("block_hash") String blockHash, @JsonProperty("previous_block_hash") @Nullable String previousBlockHash,
    @JsonProperty("gas_used") long gasUsed, List<GenesisTransaction> transactions, LinkedHashMap<String, GenesisAccount> state) {

  private static final ObjectMapper JSON = JsonMapper.builder().enable(JsonReadFeature.ALLOW_JAVA_COMMENTS).enable(JsonReadFeature.ALLOW_YAML_COMMENTS).build()
      .enable(SerializationFeature.INDENT_OUTPUT);
  private static final Path DEFAULT_PATH = Path.of("config", "genesis.json");
  private static final String GENESIS_FILE_NAME = "genesis.json";
  private static final String GENESIS_LOCK_FILE_NAME = "genesis.lock.json";
  private static final Pattern ADDRESS_PATTERN = Pattern.compile("^[0-9a-f]{40}$");
  private static final Pattern HASH_PATTERN = Pattern.compile("^[0-9a-f]{64}$");

  public GenesisParser {
    ValidationUtils.requireAllNonNull(named("transactions", transactions), named("state", state));
    ValidationUtils.requireNonNegativeLong(height, "height");
    ValidationUtils.requireNonNegativeLong(gasUsed, "gas_used");
    blockHash = ValidationUtils.requireNonBlank(blockHash, "block_hash");
    transactions = normalizeTransactions(transactions);
    state = normalizeState(state);
    validateConsistency(height, blockHash, previousBlockHash);
  }

  public static GenesisParser load(Path path) throws IOException {
    ValidationUtils.requireNonNull(path, "path");

    try (InputStream input = Files.newInputStream(path)) {
      return parse(input, "genesis file " + path);
    } catch (IOException exception) {
      throw new IOException("Failed to load genesis from " + path, exception);
    }
  }

  public static GenesisParser loadDefault() throws IOException {
    return Files.exists(DEFAULT_PATH.getParent().resolve(GENESIS_LOCK_FILE_NAME)) ? load(DEFAULT_PATH.getParent().resolve(GENESIS_LOCK_FILE_NAME)) : load(DEFAULT_PATH);
  }

  public static GenesisParser loadForConfig(Path configPath) throws IOException {
    Path lockPath = genesisLockPathForConfig(configPath);
    return Files.exists(lockPath) ? load(lockPath) : load(genesisPathForConfig(configPath));
  }

  public static Path genesisPathForConfig(Path configPath) {
    ValidationUtils.requireNonNull(configPath, "configPath");
    Path configDirectory = configPath.toAbsolutePath().normalize().getParent();
    if (configDirectory == null) {
      throw new IllegalArgumentException("configPath must have a parent directory");
    }
    return configDirectory.resolve(GENESIS_FILE_NAME);
  }

  public static Path genesisLockPathForConfig(Path configPath) {
    ValidationUtils.requireNonNull(configPath, "configPath");
    Path configDirectory = configPath.toAbsolutePath().normalize().getParent();
    if (configDirectory == null) {
      throw new IllegalArgumentException("configPath must have a parent directory");
    }
    return configDirectory.resolve(GENESIS_LOCK_FILE_NAME);
  }

  public void write(Path path) throws IOException {
    ValidationUtils.requireNonNull(path, "path");
    Path parent = path.toAbsolutePath().normalize().getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }

    try (OutputStream output = Files.newOutputStream(path)) {
      JSON.writeValue(output, this);
    } catch (IOException exception) {
      throw new IOException("Failed to write genesis to " + path, exception);
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record GenesisTransaction(String from, @Nullable String to, String amount, long nonce, @JsonProperty("gas_limit") long gasLimit, @JsonProperty("gas_price") long gasPrice,
      String input, @Nullable String signature) {

    public GenesisTransaction {
      from = ValidationUtils.requireNonBlank(from, "transaction.from");
      amount = ValidationUtils.requireNonBlank(amount, "transaction.amount");
      input = ValidationUtils.requireNonBlank(input, "transaction.input");
      ValidationUtils.requireNonNegativeLong(nonce, "transaction.nonce");
      ValidationUtils.requirePositiveLong(gasLimit, "transaction.gas_limit");
      ValidationUtils.requirePositiveLong(gasPrice, "transaction.gas_price");

      if (!ADDRESS_PATTERN.matcher(from).matches()) {
        throw new IllegalArgumentException("transaction.from must be a lowercase 40-hex-character account address");
      }

      if (to != null && !to.isBlank() && !ADDRESS_PATTERN.matcher(to).matches()) {
        throw new IllegalArgumentException("transaction.to must be null/blank or a lowercase 40-hex-character account address");
      }

      requireNonNegativeDecimalString(amount, "transaction.amount");

      if (!input.startsWith("0x")) {
        throw new IllegalArgumentException("transaction.input must use 0x-prefixed hex encoding");
      }

      boolean hasRecipient = to != null && !to.isBlank();
      boolean hasInputData = !"0x".equals(input);

      if (!hasRecipient && !hasInputData) {
        throw new IllegalArgumentException("transactions without transaction.to must include input data");
      }

      if (!hasRecipient && new BigInteger(amount).signum() != 0) {
        throw new IllegalArgumentException("contract deployment transactions must use amount 0");
      }
      if (signature != null) {
        signature = signature.trim();
      }
    }

    @JsonIgnore
    public String type() {
      if (isContractDeploy()) {
        return "CONTRACT_DEPLOY";
      }
      if (isContractCall()) {
        return "CONTRACT_CALL";
      }
      return "TRANSFER";
    }

    @JsonIgnore
    public boolean isContractDeploy() {
      return to == null || to.isBlank();
    }

    @JsonIgnore
    public boolean isContractCall() {
      return !isContractDeploy() && !"0x".equals(input);
    }

    @JsonIgnore
    public boolean isNativeTransfer() {
      return !isContractDeploy() && "0x".equals(input);
    }
  }

  public record GenesisAccount(String balance, long nonce, @Nullable String code, LinkedHashMap<String, String> storage, @Nullable AccountKind kind) {
    public GenesisAccount {
      balance = ValidationUtils.requireNonBlank(balance, "account.balance");
      ValidationUtils.requireNonNegativeLong(nonce, "account.nonce");
      storage = new LinkedHashMap<>(Objects.requireNonNullElse(storage, new LinkedHashMap<>()));

      requireNonNegativeDecimalString(balance, "account.balance");

      if (code != null && !code.isBlank() && !code.startsWith("0x")) {
        throw new IllegalArgumentException("account.code must use 0x-prefixed hex encoding when present");
      }

      if (kind == null) {
        kind = code != null && !code.isBlank() ? AccountKind.CONTRACT : AccountKind.EOA;
      }

      for (Map.Entry<String, String> entry : storage.entrySet()) {
        String key = ValidationUtils.requireNonBlank(entry.getKey(), "account.storage key");
        String value = ValidationUtils.requireNonBlank(entry.getValue(), "account.storage value");
        if (!key.startsWith("0x")) {
          throw new IllegalArgumentException("account.storage keys must use 0x-prefixed hex encoding");
        }
        if (!value.startsWith("0x")) {
          throw new IllegalArgumentException("account.storage values must use 0x-prefixed hex encoding");
        }
      }
    }
  }

  private static void requireNonNegativeDecimalString(String value, String fieldName) {
    try {
      BigInteger parsed = new BigInteger(value);
      if (parsed.signum() < 0) {
        throw new IllegalArgumentException(fieldName + " must be >= 0");
      }
    } catch (NumberFormatException exception) {
      throw new IllegalArgumentException(fieldName + " must be a decimal integer string", exception);
    }
  }

  private static GenesisParser parse(InputStream input, String sourceDescription) throws IOException {
    ValidationUtils.requireNonNull(input, "input");
    ValidationUtils.requireNonBlank(sourceDescription, "sourceDescription");

    try {
      return JSON.readValue(input, RawGenesis.class).toGenesis();
    } catch (IOException exception) {
      throw new IOException("Failed to parse " + sourceDescription, exception);
    }
  }

  private static List<GenesisTransaction> normalizeTransactions(List<GenesisTransaction> transactions) {
    return List.copyOf(ValidationUtils.requireNonNull(transactions, "transactions"));
  }

  private static LinkedHashMap<String, GenesisAccount> normalizeState(LinkedHashMap<String, GenesisAccount> state) {
    ValidationUtils.requireNonNull(state, "state");

    LinkedHashMap<String, GenesisAccount> normalizedState = new LinkedHashMap<>();
    for (Map.Entry<String, GenesisAccount> entry : state.entrySet()) {
      String address = ValidationUtils.requireNonBlank(entry.getKey(), "state address");
      if (!ADDRESS_PATTERN.matcher(address).matches()) {
        throw new IllegalArgumentException("state address must be a lowercase 40-hex-character account address: " + address);
      }

      GenesisAccount account = ValidationUtils.requireNonNull(entry.getValue(), "state account");
      normalizedState.put(address, account);
    }
    return normalizedState;
  }

  private static void validateConsistency(long height, String blockHash, @Nullable String previousBlockHash) {
    if (height != 0L) {
      throw new IllegalArgumentException("genesis.height must be 0");
    }

    if (!HASH_PATTERN.matcher(blockHash).matches()) {
      throw new IllegalArgumentException("block_hash must be a 64-char lowercase SHA-256 hex string");
    }

    if (previousBlockHash != null) {
      throw new IllegalArgumentException("genesis.previous_block_hash must be null");
    }
  }

  private record RawGenesis(long height, @JsonProperty("block_hash") String blockHash, @JsonProperty("previous_block_hash") @Nullable String previousBlockHash,
      @JsonProperty("gas_used") long gasUsed, List<GenesisTransaction> transactions, LinkedHashMap<String, GenesisAccount> state) {
    private GenesisParser toGenesis() {
      return new GenesisParser(height, blockHash, previousBlockHash, gasUsed, transactions, state);
    }
  }
}
