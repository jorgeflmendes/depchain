package pt.ulisboa.depchain.shared.network.links;

import org.eclipse.jdt.annotation.Nullable;

// Link interface that blocks until a message is received, or until a timeout occurs. (every link must implement this interface)
public interface BlockingLink<InboundType> extends AutoCloseable {
  InboundType receive() throws Exception;

  @Nullable
  InboundType receive(long timeoutMs) throws Exception;
}
