package pt.ulisboa.depchain.server.node;

import java.io.IOException;
import java.math.BigInteger;
import java.security.PublicKey;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;

import pt.ulisboa.depchain.proto.Node;
import pt.ulisboa.depchain.proto.TransactionRequest;
import pt.ulisboa.depchain.server.execution.EvmService;
import pt.ulisboa.depchain.shared.config.GenesisParser;
import pt.ulisboa.depchain.shared.crypto.CryptoUtil;
import pt.ulisboa.depchain.shared.validation.ValidationUtils;

final class ReplicaBlockPersistence {
  record RecoveryState(BlockStore.BlockDocument genesisBlock, BlockStore.BlockDocument latestBlock) {
  }

  private final BlockStore blockStore;
  private final EvmService evmService;
  private final Map<Long, Address> clientAccountAddresses;

  private BlockStore.BlockDocument latestBlock;

  ReplicaBlockPersistence(BlockStore blockStore, EvmService evmService, Map<Long, PublicKey> clientPublicKeys) {
    this.blockStore = ValidationUtils.requireNonNull(blockStore, "blockStore");
    this.evmService = ValidationUtils.requireNonNull(evmService, "evmService");
    ValidationUtils.requireNonNull(clientPublicKeys, "clientPublicKeys");
    if (clientPublicKeys.isEmpty()) {
      throw new IllegalArgumentException("clientPublicKeys must not be empty");
    }
    this.clientAccountAddresses = deriveClientAccountAddresses(clientPublicKeys);
  }

  RecoveryState initialize(GenesisParser genesis) throws IOException {
    BlockStore.BlockDocument genesisBlock = blockStore.ensureGenesisPersisted(buildGenesisBlock(genesis));
    BlockStore.BlockDocument recoveredBlock = blockStore.loadLatest().orElse(genesisBlock);
    restoreState(evmService, recoveredBlock);
    latestBlock = recoveredBlock;
    return new RecoveryState(genesisBlock, recoveredBlock);
  }

  BlockStore.BlockDocument persistExecutedNode(Node node) throws IOException {
    ValidationUtils.requireNonNull(node, "node");

    BlockStore.BlockDocument parentBlock = requireInitialized();
    BlockStore.BlockDocument nextBlock = new BlockStore.BlockDocument(parentBlock.height() + 1L, node.getNodeHash(), parentBlock.blockHash(), gasUsed(node),
        persistedTransactions(node), captureWorldState(evmService));

    blockStore.append(nextBlock);
    latestBlock = nextBlock;
    return nextBlock;
  }

  static BlockStore.BlockDocument buildGenesisBlock(GenesisParser genesis) {
    ValidationUtils.requireNonNull(genesis, "genesis");

    EvmService genesisEvm = new EvmService();
    applyGenesis(genesisEvm, genesis);
    return new BlockStore.BlockDocument(genesis.height(), genesis.blockHash(), genesis.previousBlockHash(), genesis.gasUsed(), genesis.transactions(),
        captureWorldState(genesisEvm));
  }

  static void restoreState(EvmService evmService, BlockStore.BlockDocument block) {
    ValidationUtils.requireNonNull(evmService, "evmService");
    ValidationUtils.requireNonNull(block, "block");

    for (Map.Entry<String, GenesisParser.GenesisAccount> entry : block.state().entrySet()) {
      String addressHex = entry.getKey();
      GenesisParser.GenesisAccount snapshot = entry.getValue();
      var account = evmService.createAccount(parseAddress(addressHex, "persisted state address"), snapshot.nonce(), Wei.of(new BigInteger(snapshot.balance())));

      if (snapshot.code() != null && !snapshot.code().isBlank()) {
        account.setCode(Bytes.fromHexString(snapshot.code()));
      }

      for (Map.Entry<String, String> storageEntry : snapshot.storage().entrySet()) {
        account.setStorageValue(parseStorageWordHex(storageEntry.getKey(), "persisted state storage key"), parseStorageWordHex(storageEntry
            .getValue(), "persisted state storage value"));
      }
    }
  }

  private static void applyGenesis(EvmService evmService, GenesisParser genesis) {
    for (Map.Entry<String, GenesisParser.GenesisAccount> entry : genesis.state().entrySet()) {
      String addressHex = entry.getKey();
      GenesisParser.GenesisAccount genesisAccount = entry.getValue();
      var account = evmService.createAccount(parseAddress(addressHex, "state address"), genesisAccount.nonce(), Wei.of(new BigInteger(genesisAccount.balance())));

      if (genesisAccount.code() != null && !genesisAccount.code().isBlank()) {
        account.setCode(Bytes.fromHexString(genesisAccount.code()));
      }

      for (Map.Entry<String, String> storageEntry : genesisAccount.storage().entrySet()) {
        account.setStorageValue(parseStorageWordHex(storageEntry.getKey(), "state storage key"), parseStorageWordHex(storageEntry.getValue(), "state storage value"));
      }
    }

    for (int i = 0; i < genesis.transactions().size(); i++) {
      GenesisParser.GenesisTransaction transaction = genesis.transactions().get(i);
      try {
        applyGenesisTransaction(evmService, transaction);
      } catch (RuntimeException exception) {
        throw new IllegalArgumentException("Failed to execute genesis transaction at index " + i + ": " + exception.getMessage(), exception);
      }
    }
  }

  private static void applyGenesisTransaction(EvmService evmService, GenesisParser.GenesisTransaction transaction) {
    Address from = parseAddress(transaction.from(), "transaction.from");
    Wei amount = Wei.of(new BigInteger(transaction.amount()));
    Wei gasPrice = Wei.of(transaction.gasPrice());

    switch (transaction.type()) {
      case "TRANSFER" -> {
        EvmService.TransactionResult result = evmService
            .transferNative(from, parseAddress(transaction.to(), "transaction.to"), amount, transaction.nonce(), transaction.gasLimit(), gasPrice);
        if (!result.success()) {
          throw new IllegalArgumentException(result.errorMessage());
        }
      }
      case "CONTRACT_CALL" -> {
        EvmService.TransactionResult result = evmService
            .callContract(from, parseAddress(transaction.to(), "transaction.to"), parseHexBytes(transaction.input(), "transaction.input"), amount, transaction.nonce(), transaction
                .gasLimit(), gasPrice);
        if (!result.success()) {
          throw new IllegalArgumentException(result.errorMessage());
        }
      }
      case "IST_COIN_TRANSFER" -> {
        EvmService.TransactionResult result = evmService
            .callContract(from, parseAddress(transaction.to(), "transaction.to"), parseHexBytes(transaction.input(), "transaction.input"), amount, transaction.nonce(), transaction
                .gasLimit(), gasPrice);
        if (!result.success()) {
          throw new IllegalArgumentException(result.errorMessage());
        }
      }
      case "CONTRACT_DEPLOY" -> evmService.deployContract(from, parseHexBytes(transaction.input(), "transaction.input"));
      default -> throw new IllegalArgumentException("unsupported transaction type: " + transaction.type());
    }
  }

  private static LinkedHashMap<String, GenesisParser.GenesisAccount> captureWorldState(EvmService evmService) {
    var touchedAccounts = evmService.world().getTouchedAccounts().stream().sorted(Comparator.comparing(account -> account.getAddress().toHexString())).toList();

    LinkedHashMap<String, GenesisParser.GenesisAccount> stateSnapshot = new LinkedHashMap<>();
    for (var touchedAccount : touchedAccounts) {
      String addressHex = touchedAccount.getAddress().toHexString().substring(2);
      GenesisParser.GenesisAccount accountSnapshot = snapshotAccount(evmService, addressHex);
      if (accountSnapshot != null) {
        stateSnapshot.put(addressHex, accountSnapshot);
      }
    }
    return stateSnapshot;
  }

  private List<GenesisParser.GenesisTransaction> persistedTransactions(Node node) {
    if (!node.getCommand().hasTransaction()) {
      return List.of();
    }

    TransactionRequest transaction = node.getCommand().getTransaction().getClientRequest().getTransaction();
    return List.of(toPersistedTransaction(transaction));
  }

  private static GenesisParser.GenesisAccount snapshotAccount(EvmService evmService, String addressHex) {
    var account = evmService.account(parseAddress(addressHex, "snapshot address"));
    if (account == null) {
      return null;
    }

    String code = null;
    if (account.getCode() != null && !account.getCode().isEmpty()) {
      code = account.getCode().toHexString();
    }

    LinkedHashMap<String, String> storage = new LinkedHashMap<>();
    account.getUpdatedStorage().entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> storage.put(entry.getKey().toHexString(), entry.getValue().toHexString()));

    return new GenesisParser.GenesisAccount(account.getBalance().toBigInteger().toString(), account.getNonce(), code, storage);
  }

  private GenesisParser.GenesisTransaction toPersistedTransaction(TransactionRequest transaction) {
    String type = switch (transaction.getType()) {
      case TRANSACTION_TYPE_TRANSFER -> "TRANSFER";
      case TRANSACTION_TYPE_CONTRACT_CALL -> "CONTRACT_CALL";
      case TRANSACTION_TYPE_IST_COIN_TRANSFER -> "IST_COIN_TRANSFER";
      default -> throw new IllegalArgumentException("unsupported transaction type for persistence: " + transaction.getType());
    };

    String to = null;
    if (transaction.hasTo() && !transaction.getTo().isBlank()) {
      to = transaction.getTo();
    }

    String input = "0x";
    if (transaction.hasInput() && !transaction.getInput().isEmpty()) {
      input = Bytes.wrap(transaction.getInput().toByteArray()).toHexString();
    }

    String signature = null;
    if (transaction.hasSignature() && !transaction.getSignature().isEmpty()) {
      signature = Bytes.wrap(transaction.getSignature().toByteArray()).toHexString();
    }

    String currency = switch (transaction.getType()) {
      case TRANSACTION_TYPE_TRANSFER -> "DepCoin";
      case TRANSACTION_TYPE_IST_COIN_TRANSFER -> "IST";
      case TRANSACTION_TYPE_CONTRACT_CALL -> "N/A";
      default -> throw new IllegalArgumentException("unsupported transaction type for persistence: " + transaction.getType());
    };

    return new GenesisParser.GenesisTransaction(type, currency, clientAddressFor(transaction.getRequestKey().getClientSenderId()).toHexString().substring(2), to,
        Long.toString(transaction.getAmount()), transaction.getNonce(), transaction.getGasLimit(), transaction.getGasPrice(), input, signature);
  }

  private BlockStore.BlockDocument requireInitialized() {
    if (latestBlock == null) {
      throw new IllegalStateException("Replica block persistence must be initialized before use");
    }
    return latestBlock;
  }

  private static long gasUsed(Node node) {
    return node.hasGasUsed() ? node.getGasUsed() : 0L;
  }

  private Address clientAddressFor(long clientSenderId) {
    Address clientAccountAddress = clientAccountAddresses.get(clientSenderId);
    if (clientAccountAddress != null) {
      return clientAccountAddress;
    }

    if (clientAccountAddresses.size() == 1) {
      return clientAccountAddresses.values().iterator().next();
    }
    throw new IllegalArgumentException("unknown client senderId: " + clientSenderId);
  }

  private static Map<Long, Address> deriveClientAccountAddresses(Map<Long, PublicKey> clientPublicKeys) {
    LinkedHashMap<Long, Address> addressesBySenderId = new LinkedHashMap<>();
    for (Map.Entry<Long, PublicKey> entry : clientPublicKeys.entrySet()) {
      PublicKey clientPublicKey = ValidationUtils.requireNonNull(entry.getValue(), "clientPublicKey");
      addressesBySenderId.put(entry.getKey(), Address.fromHexString("0x" + CryptoUtil.deriveAddressHex(clientPublicKey)));
    }
    return Map.copyOf(addressesBySenderId);
  }

  private static Address parseAddress(String addressHex, String fieldName) {
    if (addressHex == null || addressHex.isBlank()) {
      throw new IllegalArgumentException(fieldName + " must be a 40-char hex address");
    }
    return Address.fromHexString("0x" + addressHex);
  }

  private static Bytes parseHexBytes(String value, String fieldName) {
    try {
      return Bytes.fromHexString(value);
    } catch (RuntimeException exception) {
      throw new IllegalArgumentException(fieldName + " is not a valid hex payload", exception);
    }
  }

  private static UInt256 parseStorageWordHex(String value, String fieldName) {
    try {
      return UInt256.fromHexString(value);
    } catch (RuntimeException exception) {
      throw new IllegalArgumentException(fieldName + " is not a valid 0x-prefixed 256-bit word", exception);
    }
  }
}
