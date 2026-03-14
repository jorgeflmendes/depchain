package pt.ulisboa.depchain.shared.network.links;

import pt.ulisboa.depchain.shared.utils.ValidationUtils;

public final class LinkThreadUtil {
  private static final long SHUTDOWN_TIMEOUT_MS = 2_000L;

  private LinkThreadUtil() {
  }

  public static void awaitStop(Thread thread, String label) {
    ValidationUtils.requireNonNull(thread, "thread");
    ValidationUtils.requireNonBlank(label, "label");

    try {
      thread.join(SHUTDOWN_TIMEOUT_MS);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for thread to stop: " + label, exception);
    }

    if (thread.isAlive()) {
      throw new IllegalStateException("Timed out waiting for thread to stop: " + label);
    }
  }
}
