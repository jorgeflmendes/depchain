# DepChain (Highly Dependable Systems)

## Overview
DepChain is a permissioned blockchain project for the Highly Dependable Systems course.

This repository now contains:
- the UDP-based authenticated transport stack,
- a basic HotStuff-style consensus flow,
- threshold-signed quorum certificates,
- a simple append-only in-memory state machine with client replies.

Current scope:
- `PREPARE -> PRE_COMMIT -> COMMIT -> DECIDE` happy-path consensus,
- threshold signatures for quorum certificates,
- timeout-driven `NEW_VIEW` fallback,
- client-to-replica request/response flow.

## Current Implementation Status
Implemented:
- Configuration parsing and validation (`config/config.properties`).
- Universal `Dpch` envelope and serialization.
- UDP transport stack:
  - `FairLossLink`
  - `StubbornLink`
  - `PerfectLink`
  - `HandshakedPerfectLink`
  - `AuthenticatedLink`
- Static key generation and key loading utilities.
- Threshold key generation and loading.
- Client and server entrypoints wired to the authenticated transport stack.
- Basic HotStuff-style consensus with:
  - `NEW_VIEW`
  - `PREPARE`
  - `PRE_COMMIT`
  - `COMMIT`
  - `DECIDE`
- Threshold-signed quorum certificates.
- Timeout-based view change fallback.
- Append-only in-memory execution and client acknowledgements.
- Unit and integration tests.

Not implemented yet:
- Tests.

## System Assumptions
- Static membership (`n`, `f`, and replica set fixed before startup).
- PKI material is provisioned before execution.
- Threshold key material is provisioned before execution.
- Client library is trusted by the application.
- A subset of replicas may be Byzantine.
- UDP is the baseline transport; no TLS channels are used.
- The current liveness mechanism is timeout-based and minimal.

## Communication Stack
```text
+--------------------------------------------------+
| Client / Server / Consensus Logic                |
+--------------------------------------------------+
| APL (Authenticated Perfect Links) - implemented  |
+--------------------------------------------------+
| HPL (Handshaked Perfect Link) - implemented      |
+--------------------------------------------------+
| PL (Perfect Link semantics) - implemented        |
+--------------------------------------------------+
| SL (Stubborn retransmission) - implemented       |
+--------------------------------------------------+
| FairLossLink over UDP - implemented              |
+--------------------------------------------------+
| UDP Datagram baseline                            |
+--------------------------------------------------+
```

## Error Handling Policy
The transport stack follows a simple and uniform error-handling policy:

- Caller misuse or violated local preconditions raise exceptions (`IllegalArgumentException` or `IllegalStateException`).
- Invalid or unauthentic packets received from the network are dropped silently by the protocol layer that detects them.
- Real local failures inside a layer (for example, local send failures, serialization failures, cryptographic failures, or exhausted tracked retries) are propagated as exceptions instead of being silently ignored.

In short:
- caller error -> throw
- invalid network input -> drop
- real local layer failure -> throw

## DPCH Wire Format
```text
DPCH Frame
| magic_hi(1) | magic_lo(1) | version(1) | flags(1) | conn_id(8) | pkt_num(2) | payload(N) |
```

Field summary:
- `magic_hi`, `magic_lo` (`2 bytes`): ASCII signature `DP`.
- `version` (`1 byte`): frame version.
- `flags` (`1 byte`): semantic bits (`DATA`, `ACK`, `SYN`, `FIN`) and combinations like `SYN|ACK`, `FIN|ACK`.
- `conn_id` (`8 bytes`): logical connection identifier (`uint64`).
- `pkt_num` (`2 bytes`): per-connection packet number (`uint16`).
- `payload` (`N bytes`): remaining datagram bytes.

## Repository Layout
```text
config/
  config.properties
  keys/                         # expected key hierarchy
docs/
  hot-stuff-paper.pdf
  project.pdf
src/
  client/pt/ulisboa/depchain/client/
    DpchClient.java
    Main.java
  populate/pt/ulisboa/depchain/populate/
  server/pt/ulisboa/depchain/server/
    DpchServer.java
    Main.java
    consensus/
      Message.java
      Node.java
      QuorumCertificate.java
      Replica.java
      ViewChangeTimeoutException.java
      threshold/
        ThresholdSignatureExchange.java
        ThresholdSignatureProtocol.java
  shared/pt/ulisboa/depchain/shared/
    config/
    keys/
    network/
      dpch/
      links/
        authenticated/
        fairloss/
        handshaked/
        perfect/
        stubborn/
    utils/
  test/java/pt/ulisboa/depchain/
pom.xml
```

Maven source mapping:
- `main`: `src/server`, `src/client`, `src/populate`, `src/shared`
- `test`: `src/test/java`

## Prerequisites
- Java 21
- Maven 3.9+

Check Java:
```powershell
java -version
```

## Configuration
Main runtime configuration is in `config/config.properties`, including:
- system parameters (`n`, `f`),
- replica sender ids, endpoints, and key paths,
- threshold public key and threshold private share paths per replica,
- client sender id, settings, and request timeout,
- timeout values.

Before running:
- ensure key files exist at configured paths,
- ensure configured ports are free,
- keep config consistent across all replicas.

## Build and Test
Build everything:
```powershell
mvn clean package
```

Run unit tests:
```powershell
mvn test
```

Run unit + integration tests:
```powershell
mvn clean verify
```

## Run Locally
Populate key files from config:
```powershell
mvn exec:java@populate
```

Run one server replica:
```powershell
mvn exec:java@server -Dexec.args="server1 config/config.properties"
```

Run another replica:
```powershell
mvn exec:java@server -Dexec.args="server2 config/config.properties"
```

Run client:
```powershell
mvn exec:java@client -Dexec.args="server1 config/config.properties"
```

Client usage:
```text
Main <targetReplicaId> <configPath>
```

Server usage:
```text
Main <serverId> <configPath>
```

## References
1. HotStuff paper: <https://arxiv.org/pdf/1803.05069>
2. Springer LNCS guidelines: <https://www.springer.com/gp/computer-science/lncs/conference-proceedings-guidelines>
3. Threshold signatures library: <https://github.com/weavechain/threshold-sig>
