package pt.ulisboa.depchain.shared.config;

import static pt.ulisboa.depchain.shared.utils.ValidationUtils.named;

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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;

import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public record GenesisParser(long height, @JsonProperty("block_hash") String blockHash, @JsonProperty("previous_block_hash") @Nullable String previousBlockHash,
    @JsonProperty("gas_used") long gasUsed, List<GenesisTransaction> transactions, LinkedHashMap<String, GenesisAccount> state) {

  private static final ObjectMapper JSON = JsonMapper.builder().enable(JsonReadFeature.ALLOW_JAVA_COMMENTS).enable(JsonReadFeature.ALLOW_YAML_COMMENTS).build()
      .enable(SerializationFeature.INDENT_OUTPUT);
  private static final Path DEFAULT_PATH = Path.of("config", "genesis.json");
  private static final String GENESIS_FILE_NAME = "genesis.json";
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
    return load(DEFAULT_PATH);
  }

  public static GenesisParser loadForConfig(Path configPath) throws IOException {
    return load(genesisPathForConfig(configPath));
  }

  public static Path genesisPathForConfig(Path configPath) {
    ValidationUtils.requireNonNull(configPath, "configPath");
    Path configDirectory = configPath.toAbsolutePath().normalize().getParent();
    if (configDirectory == null) {
      throw new IllegalArgumentException("configPath must have a parent directory");
    }
    return configDirectory.resolve(GENESIS_FILE_NAME);
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

  public record GenesisTransaction(String type, String currency, String from, @Nullable String to, String amount, long nonce, @JsonProperty("gas_limit") long gasLimit,
      @JsonProperty("gas_price") long gasPrice, String input, @Nullable String signature) {

    public GenesisTransaction {
      type = ValidationUtils.requireNonBlank(type, "transaction.type");
      currency = ValidationUtils.requireNonBlank(currency, "transaction.currency");
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

      String normalizedType = type.toUpperCase();
      if (!normalizedType.equals("TRANSFER") && !normalizedType.equals("CONTRACT_CALL") && !normalizedType.equals("CONTRACT_DEPLOY")
          && !normalizedType.equals("IST_COIN_TRANSFER")) {
        throw new IllegalArgumentException("transaction.type must be TRANSFER, CONTRACT_CALL, CONTRACT_DEPLOY, or IST_COIN_TRANSFER");
      }

      if ((normalizedType.equals("TRANSFER") || normalizedType.equals("CONTRACT_CALL") || normalizedType.equals("IST_COIN_TRANSFER")) && (to == null || to.isBlank())) {
        throw new IllegalArgumentException(normalizedType + " transactions must set transaction.to");
      }

      if (normalizedType.equals("CONTRACT_DEPLOY") && to != null && !to.isBlank()) {
        throw new IllegalArgumentException("CONTRACT_DEPLOY transactions must not set transaction.to");
      }

      if (normalizedType.equals("IST_COIN_TRANSFER") && new BigInteger(amount).signum() <= 0) {
        throw new IllegalArgumentException("IST_COIN_TRANSFER transactions must use a positive amount");
      }

      if (normalizedType.equals("IST_COIN_TRANSFER") && !input.equals("0x")) {
        throw new IllegalArgumentException(normalizedType + " transactions must not include input data");
      }

      if (normalizedType.equals("TRANSFER") && !"DepCoin".equals(currency)) {
        throw new IllegalArgumentException("TRANSFER transactions must use currency DepCoin");
      }

      if (normalizedType.equals("IST_COIN_TRANSFER") && !"IST".equals(currency)) {
        throw new IllegalArgumentException("IST_COIN_TRANSFER transactions must use currency IST");
      }

      type = normalizedType;
      if (signature != null) {
        signature = signature.trim();
      }
    }
  }

  public record GenesisAccount(String balance, long nonce, @Nullable String code, LinkedHashMap<String, String> storage) {
    public GenesisAccount {
      balance = ValidationUtils.requireNonBlank(balance, "account.balance");
      ValidationUtils.requireNonNegativeLong(nonce, "account.nonce");
      storage = new LinkedHashMap<>(Objects.requireNonNullElse(storage, new LinkedHashMap<>()));

      requireNonNegativeDecimalString(balance, "account.balance");

      if (code != null && !code.isBlank() && !code.startsWith("0x")) {
        throw new IllegalArgumentException("account.code must use 0x-prefixed hex encoding when present");
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
