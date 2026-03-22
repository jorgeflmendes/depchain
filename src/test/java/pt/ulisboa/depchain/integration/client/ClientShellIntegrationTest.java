package pt.ulisboa.depchain.integration.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.hyperledger.besu.datatypes.Address;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import pt.ulisboa.depchain.client.DpchClient;
import pt.ulisboa.depchain.client.shell.DpchClientShell;
import pt.ulisboa.depchain.integration.support.IntegrationHarness;
import pt.ulisboa.depchain.integration.support.IntegrationHarness.ManagedCluster;
import pt.ulisboa.depchain.server.evm.IstCoin;

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ClientShellIntegrationTest extends IntegrationHarness {
  private static final String RECIPIENT_ADDRESS = "cccccccccccccccccccccccccccccccccccccccc";
  private static final String CLIENT_ID = "client";

  private ManagedCluster sharedCluster;

  @BeforeAll
  void startSharedCluster() throws Exception {
    sharedCluster = startManagedCluster(REPLICA_IDS);
  }

  @AfterAll
  void stopSharedCluster() throws Exception {
    if (sharedCluster != null) {
      sharedCluster.close();
    }
  }

  @Test
  @Timeout(60)
  void shellCommandsWorkEndToEnd() throws Exception {
    try (DpchClient client = DpchClient.open(sharedCluster.configPath().toString(), CLIENT_ID)) {
      String contractAddress = IstCoin.resolveDefaultContractAddress().toHexString().substring(2);
      String transferCallData = IstCoin.encodeTransferCallData(Address.fromHexString("0x" + RECIPIENT_ADDRESS), 11L).toHexString().substring(2);
      String script = String
          .join(System.lineSeparator(), "my-address", "depcoin-balance", "depcoin-transfer " + RECIPIENT_ADDRESS + " 7 0", "depcoin-balance " + RECIPIENT_ADDRESS, "ist-transfer "
              + RECIPIENT_ADDRESS
              + " 25 1", "ist-balance " + RECIPIENT_ADDRESS, "contract-call " + contractAddress + " " + transferCallData + " 2", "ist-balance " + RECIPIENT_ADDRESS, "exit")
          + System.lineSeparator();

      ByteArrayOutputStream stdoutBytes = new ByteArrayOutputStream();
      ByteArrayOutputStream stderrBytes = new ByteArrayOutputStream();
      PrintStream originalOut = System.out;
      PrintStream originalErr = System.err;
      java.io.InputStream originalIn = System.in;

      try {
        System.setIn(new ByteArrayInputStream(script.getBytes(StandardCharsets.UTF_8)));
        System.setOut(new PrintStream(stdoutBytes, true, StandardCharsets.UTF_8));
        System.setErr(new PrintStream(stderrBytes, true, StandardCharsets.UTF_8));

        new DpchClientShell(client).run();
      } finally {
        System.setIn(originalIn);
        System.setOut(originalOut);
        System.setErr(originalErr);
      }

      String stdout = stdoutBytes.toString(StandardCharsets.UTF_8);
      String stderr = stderrBytes.toString(StandardCharsets.UTF_8);

      assertTrue(stdout.contains("Wallet address: " + client.walletAddress()), stdout);
      assertTrue(stdout.contains("walletAddress=" + client.walletAddress()), stdout);
      assertTrue(stdout.contains("depcoinBalance=1000000000"), stdout);
      assertTrue(stdout.contains("depcoinBalance=7"), stdout);
      assertTrue(stdout.contains("transferResult=true"), stdout);
      assertTrue(stdout.contains("istBalance=25"), stdout);
      assertTrue(stdout.contains("return=0x"), stdout);
      assertTrue(stdout.contains("istBalance=36"), stdout);
      assertTrue(stdout.contains("Bye."), stdout);
      assertEquals("", stderr, stderr);
    }
  }
}
