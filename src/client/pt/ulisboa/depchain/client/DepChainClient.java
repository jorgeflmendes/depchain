package pt.ulisboa.depchain.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;

import pt.ulisboa.depchain.shared.config.ConfigParser;
import pt.ulisboa.depchain.shared.config.LinkConfigFactory;
import pt.ulisboa.depchain.shared.network.dpch.Dpch;
import pt.ulisboa.depchain.shared.network.links.handshaked.HandshakedPerfectLink;
import pt.ulisboa.depchain.shared.network.links.perfect.PerfectLink;
import pt.ulisboa.depchain.shared.network.model.InboundMessage;

public class DepChainClient {

  private final ConfigParser configParser;
  private final PerfectLink.BuildConfig linkConfig;

  public DepChainClient(String configPath) throws Exception {
    this.configParser = ConfigParser.load(Path.of(configPath));
    this.linkConfig = LinkConfigFactory.toBuildConfig(this.configParser);
  }

  public String append(String value, String replicaId) throws Exception {
    ConfigParser.ReplicaSection targetReplica = configParser.requireReplica(replicaId);

    InetSocketAddress targetAddress = new InetSocketAddress(
      targetReplica.host(),
      targetReplica.clientPort()
    );

    try (HandshakedPerfectLink transport = HandshakedPerfectLink.unbound(linkConfig)) {
      return sendRequest(transport, value, targetAddress);
    }
  }

  private String sendRequest(
    HandshakedPerfectLink transport,
    String value,
    InetSocketAddress targetAddress
  ) throws IOException, InterruptedException {

    long connectionId = ThreadLocalRandom.current().nextLong();

    byte[] payload = ClientCodec.encodeRequest(new ClientRequest(value));

    transport.sendReliable(connectionId, payload, targetAddress.getAddress(), targetAddress.getPort());

    try {
      while (true) {
        InboundMessage message = transport.receive();
        if (message == null) {
          continue;
        }

        Dpch packet = message.packet();

        if (packet.connectionId() == connectionId) {
          ClientReply reply = ClientCodec.decodeReply(packet.payload());
          return reply.value();
        }
      }
    } finally {
      try {
        transport.closeConnection(connectionId, targetAddress.getAddress(), targetAddress.getPort());
      } catch (RuntimeException ignored) {
      }
    }
  }
}
