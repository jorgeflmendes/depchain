package pt.ulisboa.depchain.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.keys.PrivateKeyLoader;
import pt.ulisboa.depchain.shared.keys.PublicKeyLoader;
import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.links.authenticated.AuthenticatedLink;
import pt.ulisboa.depchain.shared.network.model.InboundPacket;

public class DpchClient {
  private final ConfigParser configParser;
  private final long localSenderId;
  private final PrivateKey localStaticSKey;
  private final Map<Long, PublicKey> staticPKeys;

  public DpchClient(String configPath) throws Exception {
    this.configParser = ConfigParser.load(Path.of(configPath));
    this.localSenderId = configParser.client().senderId();
    this.localStaticSKey = PrivateKeyLoader.loadClientPrivateKey(configParser);
    this.staticPKeys = PublicKeyLoader.loadStaticPublicKeys(configParser);
  }

  public String append(String value, String replicaId) throws Exception {
    ConfigParser.ReplicaSection targetReplica = configParser.requireReplica(replicaId);
    InetSocketAddress targetAddress = new InetSocketAddress(targetReplica.host(), targetReplica.clientPort());

    try (AuthenticatedLink transport = AuthenticatedLink.unbound(localSenderId, localStaticSKey, staticPKeys)) {
      return sendRequest(transport, value, targetAddress);
    }
  }

  private String sendRequest(AuthenticatedLink transport, String value, InetSocketAddress targetAddress) throws IOException, InterruptedException {
    long connectionId = ThreadLocalRandom.current().nextLong();
    byte[] payload = ClientSerialization.encodeRequest(new ClientRequest(value));
    transport.send(connectionId, payload, targetAddress);

    try {
      while (true) {
        InboundPacket message = transport.receive();
        if (message == null) {
          continue;
        }

        Dpch packet = message.packet();

        if (packet.connectionId() == connectionId) {
          ClientReply reply = ClientSerialization.decodeReply(packet.payload());
          return reply.value();
        }
      }
    } finally {
      try {
        transport.closeConnection(connectionId, targetAddress);
      } catch (RuntimeException ignored) {
      }
    }
  }
}
