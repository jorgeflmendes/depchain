package pt.ulisboa.depchain.shared.utils;

public final class TimeUtil {
  private static final long NANOS_PER_MILLISECOND = 1_000_000L;

  public static long nowMs() {
    return System.currentTimeMillis();
  }

  public static long deadlineAfterNow(long delayMs) {
    return deadlineAfter(nowMs(), delayMs);
  }

  public static long deadlineAfter(long nowMs, long delayMs) {
    ValidationUtils.requireNonNegativeLong(delayMs, "delayMs");
    ValidationUtils.requireNonNegativeLong(nowMs, "nowMs");
    return nowMs + delayMs;
  }

  public static long remainingMsUntil(long deadlineMs) {
    return remainingMsUntil(deadlineMs, nowMs());
  }

  public static long remainingMsUntil(long deadlineMs, long nowMs) {
    ValidationUtils.requireNonNegativeLong(deadlineMs, "deadlineMs");
    ValidationUtils.requireNonNegativeLong(nowMs, "nowMs");
    return Math.max(0L, deadlineMs - nowMs);
  }

  public static boolean hasTimedOut(long deadlineMs) {
    return remainingMsUntil(deadlineMs) == 0L;
  }

  public static long monotonicNowNanos() {
    return System.nanoTime();
  }

  public static long monotonicDeadlineAfterNow(long delayMs) {
    return monotonicDeadlineAfter(monotonicNowNanos(), delayMs);
  }

  public static long monotonicDeadlineAfter(long nowNanos, long delayMs) {
    ValidationUtils.requireNonNegativeLong(nowNanos, "nowNanos");
    ValidationUtils.requireNonNegativeLong(delayMs, "delayMs");
    return nowNanos + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(delayMs);
  }

  public static long monotonicRemainingMsUntil(long deadlineNanos) {
    return monotonicRemainingMsUntil(deadlineNanos, monotonicNowNanos());
  }

  public static long monotonicRemainingMsUntil(long deadlineNanos, long nowNanos) {
    ValidationUtils.requireNonNegativeLong(deadlineNanos, "deadlineNanos");
    ValidationUtils.requireNonNegativeLong(nowNanos, "nowNanos");

    long remainingNanos = Math.max(0L, deadlineNanos - nowNanos);
    if (remainingNanos == 0L) {
      return 0L;
    }

    long remainingMs = remainingNanos / NANOS_PER_MILLISECOND;
    if (remainingMs == 0L) {
      return 1L;
    }
    return remainingMs;
  }

  public static boolean hasTimedOutMonotonic(long deadlineNanos) {
    ValidationUtils.requireNonNegativeLong(deadlineNanos, "deadlineNanos");
    return monotonicNowNanos() >= deadlineNanos;
  }

  public static boolean hasElapsedAtLeast(long nowMs, long timestampMs, long durationMs) {
    ValidationUtils.requireNonNegativeLong(nowMs, "nowMs");
    ValidationUtils.requireNonNegativeLong(timestampMs, "timestampMs");
    ValidationUtils.requireNonNegativeLong(durationMs, "durationMs");
    return (nowMs - timestampMs) >= durationMs;
  }

  public static boolean hasElapsedMoreThan(long nowMs, long timestampMs, long durationMs) {
    ValidationUtils.requireNonNegativeLong(nowMs, "nowMs");
    ValidationUtils.requireNonNegativeLong(timestampMs, "timestampMs");
    ValidationUtils.requireNonNegativeLong(durationMs, "durationMs");
    return (nowMs - timestampMs) > durationMs;
  }
}
