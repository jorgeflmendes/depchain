package pt.ulisboa.depchain.shared.logging;

public class Logger {
  void logSendError(String linkName, String hostAddress, int port, String connectionId, long sequenceNumber, int packetTypeCode, Exception exception) {
    System.out
        .printf("%s send error to %s:%d for conn=%s seq=%d type=%d = %s%n", linkName, hostAddress, port, connectionId, sequenceNumber, packetTypeCode, exception.getMessage());
  }

  // TODO: add more logging methods as needed
}
