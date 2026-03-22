package pt.ulisboa.depchain.shared.keys;

import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.PrivateKey;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class PrivateKeyLoader {
  private static final JcaPEMKeyConverter PEM_KEY_CONVERTER = new JcaPEMKeyConverter();

  private PrivateKeyLoader() {
  }

  public static PrivateKey loadReplicaPrivateKey(ConfigParser config, long senderId) throws Exception {
    ValidationUtils.requireNonNull(config, "config");
    ValidationUtils.requireNonNegativeLong(senderId, "senderId");

    return loadPrivateKey(config.requireReplicaBySenderId(senderId).privateKeyPath());
  }

  public static PrivateKey loadClientPrivateKey(ConfigParser config) throws Exception {
    ValidationUtils.requireNonNull(config, "config");
    return loadPrivateKey(config.client().privateKeyPath());
  }

  public static PrivateKey loadClientPrivateKey(ConfigParser config, long senderId) throws Exception {
    ValidationUtils.requireNonNull(config, "config");
    ValidationUtils.requireNonNegativeLong(senderId, "senderId");
    return loadPrivateKey(config.requireClientBySenderId(senderId).privateKeyPath());
  }

  public static PrivateKey decodePrivateKey(byte[] bytes) throws Exception {
    ValidationUtils.requireNonNull(bytes, "bytes");

    try (PEMParser pemParser = new PEMParser(new StringReader(new String(bytes, StandardCharsets.UTF_8)))) {
      Object parsed = pemParser.readObject();
      if (parsed instanceof PEMKeyPair pemKeyPair) {
        KeyPair keyPair = PEM_KEY_CONVERTER.getKeyPair(pemKeyPair);
        return keyPair.getPrivate();
      }
      if (parsed instanceof PrivateKeyInfo privateKeyInfo) {
        return PEM_KEY_CONVERTER.getPrivateKey(privateKeyInfo);
      }
      throw new IllegalArgumentException("Private key must be PEM-encoded");
    }
  }

  private static PrivateKey loadPrivateKey(Path path) throws Exception {
    ValidationUtils.requireNonNull(path, "path");
    return decodePrivateKey(Files.readAllBytes(path));
  }
}
