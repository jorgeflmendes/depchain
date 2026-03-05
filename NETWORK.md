# Network Stack Decision/Send Map

This document explains, for each implemented link layer, where inbound packet decisions are made and how outbound sending is performed.

## 1) FairLossLink

### Inbound decision
- Class: `pt.ulisboa.depchain.shared.network.links.fairloss.FairLossLink`
- Method: `receive()`
- Behavior: no protocol decision is made at this layer. It reads one UDP datagram from `DatagramSocket` and returns `InboundDatagram(payload, senderIp, senderPort)`.

### Send path
- Class: `FairLossLink`
- Method: `send(byte[] payload, InetAddress remoteIp, int remotePort)`
- Behavior: validates arguments and packet size, then sends one UDP datagram directly through `DatagramSocket`.

## 2) StubbornLink

### Inbound decision
- Class: `pt.ulisboa.depchain.shared.network.links.stubborn.StubbornLink`
- Method: `receive()`
- Behavior: no packet-content decision is made. It delegates to `fairLoss.receive()` and returns the datagram.

### Send path
- Classes: `StubbornLink`, `RetryRegistry`, `RetryPolicy`
- Immediate send:
1. `sendOnce(...)` sends one datagram via `fairLoss.send(...)` (no retries).
2. `sendTracked(...)` registers tracked state in `RetryRegistry`, sends once immediately, and schedules retries.
- Retry send:
1. `runRetryLoop()` waits for due retries.
2. `awaitNextRetrySend()` decides if retry is still valid, updates retry attempt/time, and returns `PendingRetry`.
3. `sendIgnoringErrors(...)` performs the retry transmit through `fairLoss.send(...)`.
- Cancellation:
1. `cancelTracked(...)` removes tracked message so future retries stop.

## 3) PerfectLink

### Inbound decision
- Class: `pt.ulisboa.depchain.shared.network.links.perfect.PerfectLink`
- Entry methods: `runReceiveLoop() -> processInbound(InboundMessage inbound)`
- Decision points:
1. `processInbound(...)` splits handling into ACK path and reliable path.
2. `handleAck(...)` decodes acknowledged type/sequence and cancels matching tracked sends (`stubbornLink.cancelTracked(...)`).
3. `handleReliable(...)` performs deduplication, window checks, reordering, and delivery queuing using `ReceiverState`.
4. `handleReliable(...)` decides whether `DATA` should be ACKed (`shouldAckData`) and sends ACK via `sendAckIgnoringErrors(...)`.

### Send path
- Class: `PerfectLink`
- API methods:
1. `send(...)` for reliable packets (`DATA`, `SYN`, `FIN`).
2. `sendAck(...)` for ACK packets.
- Internal tracked path:
1. `sendTracked(...)` chooses sequence using `SenderState` (`nextOrPendingSequence(...)`), builds `Dpch`, and calls `stubbornLink.sendTracked(...)`.
2. Control packets (`SYN`/`FIN`) may reuse pending in-flight sequence for the same type to keep one cancellation key.
- ACK path:
1. `sendAck(...)` and `sendAckIgnoringErrors(...)` send ACK datagrams with ACK payload metadata (`DATA`/`SYN`/`FIN`).

## 4) HandshakedPerfectLink

### Inbound decision
- Classes: `pt.ulisboa.depchain.shared.network.links.handshaked.HandshakedPerfectLink`, `InboundHandshakeDecider`
- Entry methods: `runReceiveLoop() -> handleInbound(InboundMessage inbound)`
- Decision flow:
1. `handleInbound(...)` filters handled types (`SYN`, `FIN`, `DATA`) and checks `ClosedConnectionsRegistry`.
2. For active connections, it obtains `ConnectionState` and calls `InboundHandshakeDecider.decideInboundLocked(...)` while synchronized on that state.
3. `InboundHandshakeDecider` routes by packet type:
   - `decideSynLocked(...)`: decides `ACK` vs `SYN_ACK` and state transitions.
   - `decideFinLocked(...)`: decides `ACK` vs `FIN_ACK` and close transitions.
   - `decideDataLocked(...)`: decides only whether data is deliverable (`state.canExchangeData()`).
4. `handleInbound(...)` executes reply via `sendControlReply(...)` and only delivers inbound `DATA` when `decision.deliverData()` is true.

### Send path
- Classes: `HandshakedPerfectLink`, `StartHandshakeCoordinator`, `CloseHandshakeCoordinator`
- Data send API:
1. `HandshakedPerfectLink.sendReliable(...)` gets per-connection state and delegates to `StartHandshakeCoordinator.sendReliable(...)`.
2. `StartHandshakeCoordinator` may send initial `SYN`, waits until fully established, then sends `DATA` through `PerfectLink.send(...)`.
- Close API:
1. `HandshakedPerfectLink.closeConnection(...)` delegates to `CloseHandshakeCoordinator`.
2. `CloseHandshakeCoordinator` waits for data drain, sends `FIN`, waits for convergence/drain, and cleans registries.
- Inbound handshake replies:
1. `sendControlReply(...)` maps `HandshakeReply` to concrete `PerfectLink` sends:
   - `ACK` -> `perfectLink.sendAck(...)`
   - `SYN_ACK` -> `perfectLink.send(..., DpchType.SYN, true, ...)`
   - `FIN_ACK` -> `perfectLink.send(..., DpchType.FIN, true, ...)`

## Summary

- `FairLossLink`: raw UDP send/receive, no protocol decisions.
- `StubbornLink`: retry scheduling/cancellation decisions for tracked sends.
- `PerfectLink`: reliability decisions (ack handling, dedupe, reorder, data ACK emission).
- `HandshakedPerfectLink`: handshake/close gating decisions (`SYN`/`FIN`/data-exchange eligibility).

## Mini Usage Guide (High-Level Consumers)

Recommended public API for application code is `HandshakedPerfectLink`.

### Setup patterns

1. Server side: `HandshakedPerfectLink.bind(bindAddress, port, buildConfig)`
2. Client side: `HandshakedPerfectLink.unbound(buildConfig)`

### Receive one message (server-style)

1. Call `receive()` (or `receive(timeoutMs)`) to get `InboundMessage`.
2. Read `inbound.packet().payload()` and sender metadata (`senderIp`, `senderPort`).
3. Reply with `sendReliable(inbound.packet().connectionId(), responsePayload, senderIp, senderPort)`.
4. Optionally close with `closeConnection(connectionId, senderIp, senderPort)` when done.

### Send one message (client-style)

1. Pick a `connectionId` (single request/response can use one random id).
2. Call `sendReliable(connectionId, payload, remoteIp, remotePort)`.
3. Wait for matching response using `receive()`/`receive(timeoutMs)` and filter by `packet.connectionId()`.
4. Close using `closeConnection(connectionId, remoteIp, remotePort)`.

### Sequence in one connection

1. Choose one `connectionId`.
2. `sendReliable(connectionId, payload1, remoteIp, remotePort)`.
3. `receive(...)` until inbound packet has `connectionId`.
4. `sendReliable(connectionId, payload2, remoteIp, remotePort)`.
5. `receive(...)` until inbound packet has `connectionId`.
6. `closeConnection(connectionId, remoteIp, remotePort)`.

### Notes

1. Reuse the same `connectionId` for all messages in the same logical session.
2. Use `receive(timeoutMs)` in higher layers to avoid blocking forever.
3. Always close the transport (`try-with-resources` or `close()`) on shutdown.
