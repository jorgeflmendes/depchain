package pt.ulisboa.depchain.shared.keys;

import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.LinkedHashMap;
import java.util.Map;

import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class PublicKeyLoader {
  private static final String KEY_ALGORITHM = "EC";
  private static final JcaPEMKeyConverter PEM_KEY_CONVERTER = new JcaPEMKeyConverter();

  private PublicKeyLoader() {
  }

  public static Map<Long, PublicKey> loadStaticPublicKeys(ConfigParser config) throws Exception {
    ValidationUtils.requireNonNull(config, "config");

    Map<Long, PublicKey> publicKeyBySenderId = new LinkedHashMap<>(loadReplicaPublicKeys(config));
    publicKeyBySenderId.put(config.client().senderId(), loadClientPublicKey(config));
    return Map.copyOf(publicKeyBySenderId);
  }

  public static Map<Long, PublicKey> loadReplicaPublicKeys(ConfigParser config) throws Exception {
    ValidationUtils.requireNonNull(config, "config");

    Map<Long, PublicKey> publicKeyBySenderId = new LinkedHashMap<>();
    for (ConfigParser.ReplicaSection replica : config.replicas()) {
      Path publicKeyPath = Path.of(replica.publicKeyPath());
      PublicKey publicKey = loadPemPublicKey(publicKeyPath);
      publicKeyBySenderId.put(replica.senderId(), publicKey);
    }

    return Map.copyOf(publicKeyBySenderId);
  }

  public static PublicKey loadClientPublicKey(ConfigParser config) throws Exception {
    ValidationUtils.requireNonNull(config, "config");
    return loadPemPublicKey(Path.of(config.client().publicKeyPath()));
  }

  public static PublicKey decodePublicKey(byte[] bytes) throws Exception {
    ValidationUtils.requireNonNull(bytes, "bytes");
    KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORITHM);
    return keyFactory.generatePublic(new X509EncodedKeySpec(bytes));
  }

  private static PublicKey loadPemPublicKey(Path path) throws Exception {
    ValidationUtils.requireNonNull(path, "path");
    byte[] bytes = Files.readAllBytes(path);

    try (PEMParser pemParser = new PEMParser(new StringReader(new String(bytes, StandardCharsets.UTF_8)))) {
      Object parsed = pemParser.readObject();
      if (parsed instanceof SubjectPublicKeyInfo publicKeyInfo) {
        return PEM_KEY_CONVERTER.getPublicKey(publicKeyInfo);
      }
      throw new IllegalArgumentException("Public key file must be PEM-encoded: " + path);
    }
  }
}
