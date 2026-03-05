package pt.ulisboa.depchain.shared.config;

import pt.ulisboa.depchain.shared.network.links.perfect.PerfectLink;
import pt.ulisboa.depchain.shared.network.links.stubborn.StubbornLink;
import pt.ulisboa.depchain.shared.utils.ValidationUtils;

// Maps parsed app config into link-layer build config objects.
public final class LinkConfigFactory {
  private LinkConfigFactory() {}

  public static PerfectLink.BuildConfig toBuildConfig(ConfigParser config) {
    ValidationUtils.requireNonNull(config, "config");
    StubbornLink.Config stubborn = toStubbornConfig(config.stubborn());
    PerfectLink.Config perfect = toPerfectConfig(config.perfect());
    int maxPacketSize = config.network().maxPacketSize();
    return new PerfectLink.BuildConfig(maxPacketSize, stubborn, perfect);
  }

  private static StubbornLink.Config toStubbornConfig(ConfigParser.StubbornSection stubborn) {
    return new StubbornLink.Config(
        stubborn.baseDelayMs(),
        stubborn.maxDelayMs(),
        stubborn.jitterRatio(),
        stubborn.maxPending(),
        stubborn.heapCompactMinSize(),
        stubborn.maxRetryAttempts(),
        stubborn.maxTrackedLifetimeMs());
  }

  private static PerfectLink.Config toPerfectConfig(ConfigParser.PerfectSection perfect) {
    return new PerfectLink.Config(
        perfect.maxWindowSize(),
        perfect.maxStreamStates(),
        perfect.streamIdleTtlMs());
  }
}
