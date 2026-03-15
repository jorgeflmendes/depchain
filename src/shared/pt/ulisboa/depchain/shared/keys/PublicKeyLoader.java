package pt.ulisboa.depchain.shared.keys;

import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PublicKey;
import java.util.LinkedHashMap;
import java.util.Map;

import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class PublicKeyLoader {
  private static final JcaPEMKeyConverter PEM_KEY_CONVERTER = new JcaPEMKeyConverter();

  private PublicKeyLoader() {
  }

  public static Map<Long, PublicKey> loadStaticPublicKeys(ConfigParser config) throws Exception {
    ValidationUtils.requireNonNull(config, "config");

    Map<Long, PublicKey> publicKeyBySenderId = new LinkedHashMap<>();
    for (ConfigParser.ReplicaSection replica : config.replicas()) {
      Path publicKeyPath = Path.of(replica.publicKeyPath());
      PublicKey publicKey = loadPublicKey(publicKeyPath);
      publicKeyBySenderId.put(replica.senderId(), publicKey);
    }

    Path clientPublicKeyPath = Path.of(config.client().publicKeyPath());
    PublicKey clientPublicKey = loadPublicKey(clientPublicKeyPath);
    publicKeyBySenderId.put(config.client().senderId(), clientPublicKey);

    return Map.copyOf(publicKeyBySenderId);
  }

  public static PublicKey decodePublicKey(byte[] bytes) throws Exception {
    ValidationUtils.requireNonNull(bytes, "bytes");

    try (PEMParser pemParser = new PEMParser(new StringReader(new String(bytes, StandardCharsets.UTF_8)))) {
      Object parsed = pemParser.readObject();
      if (parsed instanceof SubjectPublicKeyInfo publicKeyInfo) {
        return PEM_KEY_CONVERTER.getPublicKey(publicKeyInfo);
      }
      throw new IllegalArgumentException("Public key must be PEM-encoded");
    }
  }

  private static PublicKey loadPublicKey(Path path) throws Exception {
    ValidationUtils.requireNonNull(path, "path");
    return decodePublicKey(Files.readAllBytes(path));
  }
}
