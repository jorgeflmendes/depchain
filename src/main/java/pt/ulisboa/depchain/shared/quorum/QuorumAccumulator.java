package pt.ulisboa.depchain.shared.quorum;

import static pt.ulisboa.depchain.shared.validation.ValidationUtils.named;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import pt.ulisboa.depchain.shared.validation.ValidationUtils;

public final class QuorumAccumulator<S, K, V> {
  private final Set<S> allowedSenders;
  private final Set<S> acceptedSenders;
  private final Map<K, Group<S, V>> groups = new LinkedHashMap<>();

  public QuorumAccumulator() {
    this.allowedSenders = null;
    this.acceptedSenders = new LinkedHashSet<>();
  }

  public QuorumAccumulator(Set<S> allowedSenders) {
    ValidationUtils.requireNonNull(allowedSenders, "allowedSenders");
    this.allowedSenders = Set.copyOf(allowedSenders);
    this.acceptedSenders = new LinkedHashSet<>();
  }

  public boolean record(S sender, K key, V value) {
    ValidationUtils.requireAllNonNull(named("sender", sender), named("key", key), named("value", value));
    if (!accepts(sender)) {
      return false;
    }
    return groups.computeIfAbsent(key, ignored -> new Group<>()).record(sender, value);
  }

  public V recordAndGetFirstValueIfQuorumReached(S sender, K key, V value, int expectedCount) {
    ValidationUtils.requireNonNegativeInt(expectedCount, "expectedCount");
    if (!record(sender, key, value)) {
      return null;
    }
    if (hasCount(key, expectedCount)) {
      return firstValue(key);
    }
    return null;
  }

  public List<V> recordAndGetValuesIfQuorumReached(S sender, K key, V value, int expectedCount) {
    ValidationUtils.requireNonNegativeInt(expectedCount, "expectedCount");
    if (!record(sender, key, value)) {
      return List.of();
    }
    if (hasCount(key, expectedCount)) {
      return values(key);
    }
    return List.of();
  }

  public boolean hasCount(K key, int expectedCount) {
    ValidationUtils.requireNonNegativeInt(expectedCount, "expectedCount");
    return count(key) >= expectedCount;
  }

  public int count(K key) {
    ValidationUtils.requireNonNull(key, "key");
    Group<S, V> group = groups.get(key);
    if (group == null) {
      return 0;
    }
    return group.count();
  }

  public V firstValue(K key) {
    ValidationUtils.requireNonNull(key, "key");
    Group<S, V> group = groups.get(key);
    if (group == null) {
      return null;
    }
    return group.firstValue();
  }

  public List<V> values(K key) {
    ValidationUtils.requireNonNull(key, "key");
    Group<S, V> group = groups.get(key);
    if (group == null) {
      return List.of();
    }
    return group.values();
  }

  public int acceptedCount() {
    return acceptedSenders.size();
  }

  public int maxCount() {
    return groups.values().stream().mapToInt(Group::count).max().orElse(0);
  }

  private boolean accepts(S sender) {
    if (allowedSenders != null && !allowedSenders.contains(sender)) {
      return false;
    }
    return acceptedSenders.add(sender);
  }

  private static final class Group<S, V> {
    private final Map<S, V> valuesBySender = new LinkedHashMap<>();

    private boolean record(S sender, V value) {
      return valuesBySender.putIfAbsent(sender, value) == null;
    }

    private int count() {
      return valuesBySender.size();
    }

    private V firstValue() {
      if (valuesBySender.isEmpty()) {
        return null;
      }
      return valuesBySender.values().iterator().next();
    }

    private List<V> values() {
      return new ArrayList<>(valuesBySender.values());
    }
  }
}
