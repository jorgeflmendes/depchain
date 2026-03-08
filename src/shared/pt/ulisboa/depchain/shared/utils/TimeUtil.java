package pt.ulisboa.depchain.shared.utils;

public final class TimeUtil {
  public static long nowMs() {
    return System.currentTimeMillis();
  }

  public static long deadlineAfter(long delayMs) {
    return deadlineAfter(nowMs(), delayMs);
  }

  public static long deadlineAfter(long nowMs, long delayMs) {
    ValidationUtils.requireNonNegativeLong(nowMs, "nowMs");
    ValidationUtils.requireNonNegativeLong(delayMs, "delayMs");
    return nowMs + delayMs;
  }

  public static boolean hasReachedDeadline(long deadlineMs) {
    return hasReachedDeadline(nowMs(), deadlineMs);
  }

  public static boolean hasReachedDeadline(long nowMs, long deadlineMs) {
    ValidationUtils.requireNonNegativeLong(nowMs, "nowMs");
    ValidationUtils.requireNonNegativeLong(deadlineMs, "deadlineMs");
    return nowMs >= deadlineMs;
  }

  public static long remainingMsUntil(long deadlineMs) {
    return remainingMsUntil(deadlineMs, nowMs());
  }

  public static long remainingMsUntil(long deadlineMs, long nowMs) {
    ValidationUtils.requireNonNegativeLong(deadlineMs, "deadlineMs");
    ValidationUtils.requireNonNegativeLong(nowMs, "nowMs");
    return Math.max(0L, deadlineMs - nowMs);
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
