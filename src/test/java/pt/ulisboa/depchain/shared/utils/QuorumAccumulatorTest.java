package pt.ulisboa.depchain.shared.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

class QuorumAccumulatorTest {
  @Test
  void deduplicatesSendersAcrossOneCollectionRound() {
    QuorumAccumulator<Integer, String, String> accumulator = new QuorumAccumulator<>();

    assertTrue(accumulator.record(1, "ok", "reply-a"));
    assertFalse(accumulator.record(1, "ok", "reply-b"));
    assertTrue(accumulator.record(2, "ok", "reply-c"));

    assertEquals(2, accumulator.count("ok"));
    assertTrue(accumulator.hasCount("ok", 2));
    assertEquals("reply-a", accumulator.firstValue("ok"));
    assertIterableEquals(java.util.List.of("reply-a", "reply-c"), accumulator.values("ok"));
  }

  @Test
  void returnsQuorumValueWhenThresholdIsReached() {
    QuorumAccumulator<Integer, String, String> accumulator = new QuorumAccumulator<>();

    assertNull(accumulator.recordAndGetFirstValueIfQuorumReached(1, "ok", "reply-a", 2));
    assertEquals("reply-a", accumulator.recordAndGetFirstValueIfQuorumReached(2, "ok", "reply-b", 2));
  }

  @Test
  void canRestrictAcceptedSenders() {
    QuorumAccumulator<Integer, String, String> accumulator = new QuorumAccumulator<>(Set.of(1, 2));

    assertFalse(accumulator.record(3, "ok", "reply-x"));
    assertIterableEquals(List.of(), accumulator.recordAndGetValuesIfQuorumReached(1, "ok", "reply-a", 2));
    assertIterableEquals(List.of("reply-a", "reply-b"), accumulator.recordAndGetValuesIfQuorumReached(2, "ok", "reply-b", 2));
  }
}
