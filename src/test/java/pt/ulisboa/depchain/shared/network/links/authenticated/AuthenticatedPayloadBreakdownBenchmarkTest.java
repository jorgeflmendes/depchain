package pt.ulisboa.depchain.shared.network.links.authenticated;

import java.nio.charset.StandardCharsets;
import java.security.KeyPair;

import javax.crypto.SecretKey;

import org.junit.jupiter.api.Test;

import pt.ulisboa.depchain.proto.AuthOpcode;
import pt.ulisboa.depchain.proto.AuthenticatedDataEnvelope;
import pt.ulisboa.depchain.proto.AuthenticatedHandshakeEnvelope;
import pt.ulisboa.depchain.shared.utils.CryptoUtil;

class AuthenticatedPayloadBreakdownBenchmarkTest {
  private static final int WARMUP_ITERATIONS = 2_000;
  private static final int MEASURED_ITERATIONS = 20_000;

  @Test
  void benchmarkAuthenticatedPayloadHotPath() throws Exception {
    KeyPair senderStaticKeys = CryptoUtil.newECKeyPair();
    KeyPair receiverStaticKeys = CryptoUtil.newECKeyPair();
    KeyPair ephemeralKeys = CryptoUtil.newECKeyPair();
    SecretKey sharedSecret = CryptoUtil.deriveCommonKey(senderStaticKeys.getPrivate(), receiverStaticKeys.getPublic(), new CryptoUtil.KeyContext("authenticated", "bench"));
    byte[] payload = "authenticated-payload-benchmark".getBytes(StandardCharsets.UTF_8);

    byte[] encodedHandshake = AuthenticatedPayloadUtil.encodeEcdsa(AuthOpcode.AUTH_OPCODE_INIT, 1L, ephemeralKeys.getPublic(), senderStaticKeys.getPrivate());
    byte[] encodedData = AuthenticatedPayloadUtil.encodeHmac(AuthOpcode.AUTH_OPCODE_DATA, payload, sharedSecret, 7L);
    AuthenticatedHandshakeEnvelope decodedHandshake = AuthenticatedPayloadUtil.decodeEcdsa(encodedHandshake);
    AuthenticatedDataEnvelope decodedData = AuthenticatedPayloadUtil.decodeHmac(encodedData);

    runHandshakeEncode(senderStaticKeys, ephemeralKeys, WARMUP_ITERATIONS);
    runHandshakeDecode(encodedHandshake, WARMUP_ITERATIONS);
    runHandshakeVerify(decodedHandshake, senderStaticKeys, WARMUP_ITERATIONS);
    runDataEncode(sharedSecret, payload, WARMUP_ITERATIONS);
    runDataDecode(encodedData, WARMUP_ITERATIONS);
    runDataVerify(decodedData, sharedSecret, WARMUP_ITERATIONS);

    System.out.println("AUTH_BREAKDOWN handshake_encode_ms=" + runHandshakeEncode(senderStaticKeys, ephemeralKeys, MEASURED_ITERATIONS));
    System.out.println("AUTH_BREAKDOWN handshake_decode_ms=" + runHandshakeDecode(encodedHandshake, MEASURED_ITERATIONS));
    System.out.println("AUTH_BREAKDOWN handshake_verify_ms=" + runHandshakeVerify(decodedHandshake, senderStaticKeys, MEASURED_ITERATIONS));
    System.out.println("AUTH_BREAKDOWN data_encode_ms=" + runDataEncode(sharedSecret, payload, MEASURED_ITERATIONS));
    System.out.println("AUTH_BREAKDOWN data_decode_ms=" + runDataDecode(encodedData, MEASURED_ITERATIONS));
    System.out.println("AUTH_BREAKDOWN data_verify_ms=" + runDataVerify(decodedData, sharedSecret, MEASURED_ITERATIONS));
    System.out.println("AUTH_BREAKDOWN iterations=" + MEASURED_ITERATIONS);
  }

  private static double runHandshakeEncode(KeyPair staticKeys, KeyPair ephemeralKeys, int iterations) throws Exception {
    long startedAt = System.nanoTime();
    for (int i = 0; i < iterations; i++) {
      AuthenticatedPayloadUtil.encodeEcdsa(AuthOpcode.AUTH_OPCODE_INIT, 1L, ephemeralKeys.getPublic(), staticKeys.getPrivate());
    }
    return elapsedMs(startedAt);
  }

  private static double runHandshakeDecode(byte[] encodedHandshake, int iterations) {
    long startedAt = System.nanoTime();
    for (int i = 0; i < iterations; i++) {
      AuthenticatedPayloadUtil.decodeEcdsa(encodedHandshake);
    }
    return elapsedMs(startedAt);
  }

  private static double runHandshakeVerify(AuthenticatedHandshakeEnvelope decodedHandshake, KeyPair staticKeys, int iterations) throws Exception {
    long startedAt = System.nanoTime();
    for (int i = 0; i < iterations; i++) {
      AuthenticatedPayloadUtil.verifyEcdsa(decodedHandshake, staticKeys.getPublic());
    }
    return elapsedMs(startedAt);
  }

  private static double runDataEncode(SecretKey sharedSecret, byte[] payload, int iterations) throws Exception {
    long startedAt = System.nanoTime();
    for (int i = 0; i < iterations; i++) {
      AuthenticatedPayloadUtil.encodeHmac(AuthOpcode.AUTH_OPCODE_DATA, payload, sharedSecret, i);
    }
    return elapsedMs(startedAt);
  }

  private static double runDataDecode(byte[] encodedData, int iterations) {
    long startedAt = System.nanoTime();
    for (int i = 0; i < iterations; i++) {
      AuthenticatedPayloadUtil.decodeHmac(encodedData);
    }
    return elapsedMs(startedAt);
  }

  private static double runDataVerify(AuthenticatedDataEnvelope decodedData, SecretKey sharedSecret, int iterations) throws Exception {
    long startedAt = System.nanoTime();
    for (int i = 0; i < iterations; i++) {
      AuthenticatedPayloadUtil.verifyHmac(decodedData, sharedSecret);
    }
    return elapsedMs(startedAt);
  }

  private static double elapsedMs(long startedAt) {
    return (System.nanoTime() - startedAt) / 1_000_000.0;
  }
}
