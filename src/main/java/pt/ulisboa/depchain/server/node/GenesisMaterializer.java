package pt.ulisboa.depchain.server.node;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.hyperledger.besu.datatypes.Address;

import pt.ulisboa.depchain.server.execution.IstCoin;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.config.GenesisParser;
import pt.ulisboa.depchain.shared.crypto.CryptoUtil;
import pt.ulisboa.depchain.shared.model.AccountKind;
import pt.ulisboa.depchain.shared.validation.ValidationUtils;

final class GenesisMaterializer {
  private static final String ERC20_TRANSFER_SELECTOR = "a9059cbb";

  private GenesisMaterializer() {
  }

  static GenesisParser loadOrWriteLock(Path configPath, ConfigParser config, Map<Long, PublicKey> clientPublicKeys) throws java.io.IOException {
    ValidationUtils.requireNonNull(configPath, "configPath");
    ValidationUtils.requireNonNull(config, "config");
    ValidationUtils.requireNonNull(clientPublicKeys, "clientPublicKeys");

    Path lockPath = GenesisParser.genesisLockPathForConfig(configPath);
    if (Files.exists(lockPath)) {
      return GenesisParser.load(lockPath);
    }

    GenesisParser lockedGenesis = materialize(GenesisParser.load(GenesisParser.genesisPathForConfig(configPath)), config, clientPublicKeys);
    lockedGenesis.write(lockPath);
    return lockedGenesis;
  }

  static GenesisParser materialize(GenesisParser template, ConfigParser config, Map<Long, PublicKey> clientPublicKeys) {
    ValidationUtils.requireNonNull(template, "template");
    ValidationUtils.requireNonNull(config, "config");
    ValidationUtils.requireNonNull(clientPublicKeys, "clientPublicKeys");

    List<String> clientAddresses = configuredClientAddresses(config, clientPublicKeys);
    LinkedHashMap<String, GenesisParser.GenesisAccount> state = new LinkedHashMap<>(template.state());
    for (String clientAddress : clientAddresses) {
      state.putIfAbsent(clientAddress, new GenesisParser.GenesisAccount("0", 0L, null, new LinkedHashMap<>(), AccountKind.EOA));
    }

    List<GenesisParser.GenesisTransaction> materializedTransactions = new ArrayList<>(template.transactions().size());
    int nextFundedClientIndex = 0;
    boolean assignedIstBootstrapRecipient = false;
    for (GenesisParser.GenesisTransaction transaction : template.transactions()) {
      if (!assignedIstBootstrapRecipient && isIstBootstrapTransfer(transaction) && !clientAddresses.isEmpty()) {
        String recipientAddress = clientAddresses.getFirst();
        long transferAmount = decodeIstTransferAmount(transaction.input());
        String input = IstCoin.encodeTransferCallData(Address.fromHexString("0x" + recipientAddress), transferAmount).toHexString();
        materializedTransactions.add(copyTransaction(transaction, transaction.to(), input));
        assignedIstBootstrapRecipient = true;
        continue;
      }

      if (isDepCoinBootstrapTransfer(transaction) && nextFundedClientIndex < clientAddresses.size()) {
        String recipientAddress = clientAddresses.get(nextFundedClientIndex++);
        materializedTransactions.add(copyTransaction(transaction, recipientAddress, transaction.input()));
        continue;
      }

      materializedTransactions.add(transaction);
    }

    return new GenesisParser(template.height(), template.blockHash(), template.previousBlockHash(), template.gasUsed(), materializedTransactions, state);
  }

  private static List<String> configuredClientAddresses(ConfigParser config, Map<Long, PublicKey> clientPublicKeys) {
    List<String> clientAddresses = new ArrayList<>(config.clients().size());
    for (ConfigParser.ClientSection client : config.clients()) {
      PublicKey clientPublicKey = clientPublicKeys.get(client.senderId());
      if (clientPublicKey == null) {
        throw new IllegalArgumentException("Missing public key for configured client senderId " + client.senderId());
      }
      clientAddresses.add(CryptoUtil.deriveAddressHex(clientPublicKey));
    }
    return List.copyOf(clientAddresses);
  }

  private static boolean isDepCoinBootstrapTransfer(GenesisParser.GenesisTransaction transaction) {
    return "TRANSFER".equals(transaction.type()) && "DepCoin".equals(transaction.currency());
  }

  private static boolean isIstBootstrapTransfer(GenesisParser.GenesisTransaction transaction) {
    if (!"CONTRACT_CALL".equals(transaction.type()) || !"IST".equals(transaction.currency())) {
      return false;
    }
    String input = transaction.input();
    return input != null && input.startsWith("0x" + ERC20_TRANSFER_SELECTOR);
  }

  private static long decodeIstTransferAmount(String input) {
    String normalizedInput = ValidationUtils.requireNonBlank(input, "input");
    if (!normalizedInput.startsWith("0x")) {
      throw new IllegalArgumentException("IST transfer input must be 0x-prefixed");
    }

    String hex = normalizedInput.substring(2);
    int expectedLength = 8 + 64 + 64;
    if (hex.length() != expectedLength || !hex.startsWith(ERC20_TRANSFER_SELECTOR)) {
      throw new IllegalArgumentException("Unsupported IST transfer bootstrap calldata");
    }

    String amountHex = hex.substring(8 + 64);
    return new BigInteger(amountHex, 16).longValueExact();
  }

  private static GenesisParser.GenesisTransaction copyTransaction(GenesisParser.GenesisTransaction original, String to, String input) {
    return new GenesisParser.GenesisTransaction(original.type(), original.currency(), original.from(), to, original.amount(), original.nonce(), original.gasLimit(),
        original.gasPrice(), input, original.signature());
  }
}
