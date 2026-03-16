package pt.ulisboa.depchain.integration.client;

import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import pt.ulisboa.depchain.integration.support.IntegrationHarness;
import pt.ulisboa.depchain.integration.support.IntegrationHarness.StartedServer;
import pt.ulisboa.depchain.proto.AppendRequest;
import pt.ulisboa.depchain.proto.ClientRequest;
import pt.ulisboa.depchain.proto.ClientRequestKey;
import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.keys.PrivateKeyLoader;
import pt.ulisboa.depchain.shared.keys.PublicKeyLoader;

@Tag("integration")
class MaliciousClientIntegrationTest extends IntegrationHarness {
  @Test
  @Timeout(30)
  void clientCannotAuthenticateOnConsensusPortTest() throws Exception {
    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    List<StartedServer> servers = startServers(REPLICA_IDS, configPath);
    try {
      waitForServersStartup(servers, STARTUP_TIMEOUT);

      ConfigParser config = ConfigParser.load(configPath);
      PrivateKey clientPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config);
      Map<Long, PublicKey> replicaPublicKeys = PublicKeyLoader.loadReplicaPublicKeys(config);
      ClientRequest request = signedRequest(configPath, "consensus-port-reject");
      byte[] payload = request.toByteArray();

      assertNull(sendPayloadToConsensusPort(configPath, LEADER_REPLICA_ID, config.client().senderId(), clientPrivateKey, replicaPublicKeys, payload, Duration
          .ofSeconds(3)), "A client must not be able to authenticate on the consensus port");
      assertRequestSucceeds(configPath, "post-consensus-port-reject", STANDARD_REQUEST_TIMEOUT, servers, "Cluster should remain responsive after rejecting client traffic on the consensus port");
    } finally {
      stopProcesses(servers);
    }
  }

  @Test
  @Timeout(30)
  void clientRequestWithWrongSenderIdRejectedTest() throws Exception {
    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    List<StartedServer> servers = startServers(REPLICA_IDS, configPath);
    try {
      waitForServersStartup(servers, STARTUP_TIMEOUT);

      ConfigParser config = ConfigParser.load(configPath);
      PrivateKey clientPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config);
      Map<Long, PublicKey> staticPublicKeys = PublicKeyLoader.loadStaticPublicKeys(config);
      ClientRequest forgedSenderRequest = signedAppendRequest(config.client().senderId() + 1, 991L, "wrong-sender", clientPrivateKey);

      assertNull(sendPayloadToClientPort(configPath, LEADER_REPLICA_ID, config.client().senderId(), clientPrivateKey, staticPublicKeys, forgedSenderRequest.toByteArray(), Duration
          .ofSeconds(3)), "Requests with a mismatched client sender id must be rejected");
      assertRequestSucceeds(configPath, "post-wrong-sender", STANDARD_REQUEST_TIMEOUT, servers, "Cluster should remain responsive after rejecting a forged client sender id");
    } finally {
      stopProcesses(servers);
    }
  }

  @Test
  @Timeout(30)
  void malformedClientRequestPayloadRejectedTest() throws Exception {
    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    List<StartedServer> servers = startServers(REPLICA_IDS, configPath);
    try {
      waitForServersStartup(servers, STARTUP_TIMEOUT);

      ConfigParser config = ConfigParser.load(configPath);
      PrivateKey clientPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config);
      Map<Long, PublicKey> staticPublicKeys = PublicKeyLoader.loadStaticPublicKeys(config);

      assertNull(sendPayloadToClientPort(configPath, LEADER_REPLICA_ID, config.client().senderId(), clientPrivateKey, staticPublicKeys, new byte[]{1, 2, 3, 4}, Duration
          .ofSeconds(3)), "Malformed protobuf client payloads must be rejected");
      assertRequestSucceeds(configPath, "post-malformed-payload", STANDARD_REQUEST_TIMEOUT, servers, "Cluster should remain responsive after malformed client payloads");
    } finally {
      stopProcesses(servers);
    }
  }

  @Test
  @Timeout(30)
  void protoInvalidClientRequestRejectedTest() throws Exception {
    Path configPath = integrationConfigPath();
    populateConfig(configPath);

    List<StartedServer> servers = startServers(REPLICA_IDS, configPath);
    try {
      waitForServersStartup(servers, STARTUP_TIMEOUT);

      ConfigParser config = ConfigParser.load(configPath);
      PrivateKey clientPrivateKey = PrivateKeyLoader.loadClientPrivateKey(config);
      Map<Long, PublicKey> staticPublicKeys = PublicKeyLoader.loadStaticPublicKeys(config);
      ClientRequest invalidRequest = ClientRequest.newBuilder().setAppend(AppendRequest.newBuilder()
          .setRequestKey(ClientRequestKey.newBuilder().setClientSenderId(config.client().senderId()).setRequestId(992L)).setValue("missing-signature")).build();

      assertNull(sendPayloadToClientPort(configPath, LEADER_REPLICA_ID, config.client().senderId(), clientPrivateKey, staticPublicKeys, invalidRequest.toByteArray(), Duration
          .ofSeconds(3)), "Proto-valid but protovalidate-invalid client requests must be rejected");
      assertRequestSucceeds(configPath, "post-invalid-client-proto", STANDARD_REQUEST_TIMEOUT, servers, "Cluster should remain responsive after rejecting invalid client protos");
    } finally {
      stopProcesses(servers);
    }
  }
}
