package pt.ulisboa.depchain.shared.network.links;

// Link interface that blocks until a message is received, or until a timeout occurs. (every link must implement this interface)
public interface BlockingLink<InboundType> extends AutoCloseable {
  InboundType receive() throws Exception;

  InboundType receive(long timeoutMs) throws Exception;
}
