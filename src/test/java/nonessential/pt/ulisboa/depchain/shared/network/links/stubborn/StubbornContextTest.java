package pt.ulisboa.depchain.shared.network.links.stubborn;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import pt.ulisboa.depchain.shared.network.links.fairloss.FairLossLink;

final class StubbornContextTest {
  @Test
  void retryDelayRemainsBoundedForLargeAttempts() throws IOException {
    try (FairLossLink fairLossLink = FairLossLink.unbound()) {
      StubbornContext context = new StubbornContext(fairLossLink);
      try {
        long delayMs = assertDoesNotThrow(() -> context.retryDelayMs(256));
        assertTrue(delayMs >= 1L);
        assertTrue(delayMs <= StubbornLink.DEFAULT_MAX_DELAY_MS);
      } finally {
        context.retryTimer.stop();
      }
    }
  }
}
