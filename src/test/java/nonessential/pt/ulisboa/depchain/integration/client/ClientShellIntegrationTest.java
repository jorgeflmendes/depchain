package pt.ulisboa.depchain.integration.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.hyperledger.besu.datatypes.Address;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pt.ulisboa.depchain.client.api.ClientReplicaApi;
import pt.ulisboa.depchain.client.cli.ClientShell;
import pt.ulisboa.depchain.integration.support.ClusterIntegrationTestBase;
import pt.ulisboa.depchain.server.execution.IstCoin;

@Tag("integration")
@DisplayName("Client shell integration")
class ClientShellIntegrationTest extends ClusterIntegrationTestBase {
  private static final String ANSI_ESCAPE_PATTERN = "\\u001B\\[[;\\d]*m";
  private static final String RECIPIENT_ADDRESS = "cccccccccccccccccccccccccccccccccccccccc";
  private static final String CLIENT_ID = "client";

  @Test
  void shellCommandsWorkEndToEnd() throws Exception {
    try (ClientReplicaApi client = ClientReplicaApi.connect(cluster().configPath().toString(), CLIENT_ID)) {
      String contractAddress = IstCoin.resolveContractAddress(cluster().configPath()).toHexString().substring(2);
      String transferCallData = IstCoin.encodeTransferCallData(Address.fromHexString("0x" + RECIPIENT_ADDRESS), 11L).toHexString().substring(2);
      String script = String
          .join(System.lineSeparator(), "my-address", "depcoin-balance", "depcoin-transfer " + RECIPIENT_ADDRESS + " 7 0", "depcoin-balance " + RECIPIENT_ADDRESS, "ist-transfer "
              + RECIPIENT_ADDRESS + " 25 1", "contract-call " + contractAddress + " " + transferCallData + " 2", "ist-balance " + RECIPIENT_ADDRESS, "exit")
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

        new ClientShell(client).run();
      } finally {
        System.setIn(originalIn);
        System.setOut(originalOut);
        System.setErr(originalErr);
      }

      String stdout = stdoutBytes.toString(StandardCharsets.UTF_8).replaceAll(ANSI_ESCAPE_PATTERN, "");
      String stderr = stderrBytes.toString(StandardCharsets.UTF_8);

      assertThat(stdout).contains("DEPCHAIN CLIENT CONSOLE");
      assertThat(stdout).contains("Active Wallet  " + client.getWalletAddress());
      assertThat(stdout).contains("walletAddress=" + client.getWalletAddress());
      assertThat(stdout).contains("depcoinBalance=1000000000");
      assertThat(stdout).contains("depcoinBalance=7");
      assertThat(stdout).contains("transferResult=true");
      assertThat(stdout).contains("return=0x");
      assertThat(stdout).contains("istBalance=36");
      assertThat(stdout).contains("Session closed.");
      assertThat(stderr).isEmpty();
    }
  }
}
