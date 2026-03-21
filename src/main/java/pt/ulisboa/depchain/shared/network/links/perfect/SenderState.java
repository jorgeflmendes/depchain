package pt.ulisboa.depchain.shared.network.links.perfect;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.ulisboa.depchain.proto.DpchPacketType;
import pt.ulisboa.depchain.shared.network.links.LinkFailureException;
import pt.ulisboa.depchain.shared.network.links.stubborn.tracking.TrackedKey;
import pt.ulisboa.depchain.shared.network.packet.DpchPacketUtil;
import pt.ulisboa.depchain.shared.utils.TimeUtil;

final class SenderState {
  private static final Logger logger = LoggerFactory.getLogger(SenderState.class);
  private static final int DATA_PACKET_TYPE_NUMBER = DpchPacketType.DPCH_PACKET_TYPE_DATA.getNumber();
  private static final int NEAR_EXHAUSTION_THRESHOLD = 1024;

  private int nextSequence;
  private final BitSet inFlightSequences = new BitSet();
  private int inFlightCount;
  private boolean nearExhaustionLogged;

  synchronized int nextSequence() {
    if (!nearExhaustionLogged && nextSequence >= DpchPacketUtil.MAX_PACKET_NUMBER - NEAR_EXHAUSTION_THRESHOLD) {
      nearExhaustionLogged = true;
      logger.warn("Sender sequence number nearing exhaustion for stream. nextSequence={}, inFlight={}", nextSequence, inFlightCount);
    }

    if (nextSequence > DpchPacketUtil.MAX_PACKET_NUMBER) {
      throw new IllegalStateException("Sender sequence number exhausted for stream");
    }

    int sequence = nextSequence++;
    inFlightSequences.set(sequence);
    inFlightCount++;
    notifyAll();
    return sequence;
  }

  synchronized TrackedKey acknowledge(long connectionId, int sequenceNumber) {
    if (!inFlightSequences.get(sequenceNumber)) {
      return null;
    }

    inFlightSequences.clear(sequenceNumber);
    inFlightCount--;
    notifyAll();
    return new TrackedKey(connectionId, sequenceNumber, DATA_PACKET_TYPE_NUMBER);
  }

  synchronized List<TrackedKey> cancelPendingData(long connectionId) {
    List<TrackedKey> cancellations = new ArrayList<>(inFlightCount);
    for (int sequenceNumber = inFlightSequences.nextSetBit(0); sequenceNumber >= 0; sequenceNumber = inFlightSequences.nextSetBit(sequenceNumber + 1)) {
      cancellations.add(new TrackedKey(connectionId, sequenceNumber, DATA_PACKET_TYPE_NUMBER));
    }

    inFlightSequences.clear();
    inFlightCount = 0;
    notifyAll();
    return cancellations;
  }

  synchronized boolean waitUntilNoPendingData(PerfectContext context, long connectionId, InetSocketAddress remoteEndpoint, long deadlineMs) throws InterruptedException {
    while (context.isRunning()) {
      LinkFailureException failure = pollTerminalFailure(context, connectionId, remoteEndpoint);
      if (failure != null) {
        throw failure;
      }

      if (inFlightCount == 0) {
        return true;
      }

      long remainingMs = TimeUtil.remainingMsUntil(deadlineMs);
      if (remainingMs <= 0L) {
        return false;
      }
      wait(remainingMs);
    }
    return inFlightSequences.isEmpty();
  }

  synchronized void notifyWaiters() {
    notifyAll();
  }

  synchronized LinkFailureException pollTerminalFailure(PerfectContext context, long connectionId, InetSocketAddress remoteEndpoint) {
    for (int sequenceNumber = inFlightSequences.nextSetBit(0); sequenceNumber >= 0; sequenceNumber = inFlightSequences.nextSetBit(sequenceNumber + 1)) {
      LinkFailureException failure = context.stubbornLink.pollTerminalFailure(new TrackedKey(connectionId, sequenceNumber, DATA_PACKET_TYPE_NUMBER), remoteEndpoint);
      if (failure != null) {
        return failure;
      }
    }

    return null;
  }
}
