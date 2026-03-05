package pt.ulisboa.depchain.client;

import java.io.IOException;
import java.net.InetAddress;
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
        
        InetAddress targetAddress = InetAddress.getByName(targetReplica.host());
        int targetPort = targetReplica.clientPort();

        long timeout = configParser.client().requestTimeoutMs();

        try (HandshakedPerfectLink transport = HandshakedPerfectLink.unbound(linkConfig)) {
            return sendRequest(transport, value, targetAddress, targetPort, timeout);
        }
    }

    private String sendRequest(
        HandshakedPerfectLink transport,
        String value,
        InetAddress targetAddress,
        int targetPort,
        long timeout
    ) throws IOException, InterruptedException {
        
        long connectionId = ThreadLocalRandom.current().nextLong();

        byte[] payload = ClientCodec.encodeRequest(new ClientRequest(value));

        transport.sendReliable(connectionId, payload, targetAddress, targetPort);

        long deadline = System.currentTimeMillis() + timeout;

        try {
            while (true) {

                long remainingTime = deadline - System.currentTimeMillis();
                if (remainingTime <= 0) {
                    throw new IOException("Request timed out");
                }

                InboundMessage message = transport.receive(remainingTime);
                if (message == null) {
                    throw new IOException("Request timed out");
                }

                Dpch packet = message.packet();

                if (packet.connectionId() == connectionId) {
                    Clientreply reply = ClientCodec.decodeReply(packet.payload());
                    return reply.value();
                }
            }
        } finally {
            try {
                transport.closeConnection(connectionId, targetAddress, targetPort);
            } catch (RuntimeException ignored) {
            }
        } 
    }
}
