package pt.ulisboa.depchain.shared.config;

import java.util.Objects;

import pt.ulisboa.depchain.shared.network.links.perfect.PerfectLink;
import pt.ulisboa.depchain.shared.network.links.stubborn.StubbornLink;

// Maps parsed app config into link-layer build config objects.
public final class LinkConfigFactory {
  private LinkConfigFactory() {}

  public static PerfectLink.BuildConfig from(ConfigParser config) {
    Objects.requireNonNull(config, "config cannot be null");
    ConfigParser.StubbornSection stubborn = config.stubborn();
    ConfigParser.PerfectSection perfect = config.perfect();

    StubbornLink.Config stubbornConfig = new StubbornLink.Config(stubborn.baseDelayMs(), stubborn.maxDelayMs(), stubborn.jitterRatio(), stubborn.maxPending(), stubborn.heapCompactMinSize(), stubborn.maxRetryAttempts(), stubborn.maxTrackedLifetimeMs());
    PerfectLink.Config perfectConfig = new PerfectLink.Config(perfect.maxWindowSize(), perfect.maxStreamStates(), perfect.streamIdleTtlMs());
    return new PerfectLink.BuildConfig(config.network().maxPacketSize(), stubbornConfig, perfectConfig);
  }
}
