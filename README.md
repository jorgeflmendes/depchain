# DepChain (Highly Dependable Systems) - Stage 1

## Overview
DepChain is a permissioned blockchain project for the Highly Dependable Systems course.  
This repository contains the Stage 1 networking and transport foundation used by client and server processes.

Stage 1 target scope:
- Basic HotStuff consensus (Algorithm 2 from the HotStuff paper).
- Client-to-replica request/response flow.
- Append-only in-memory state machine for committed values.

## Current Implementation Status
Implemented:
- Configuration parsing and validation (`config/config.yaml`, parser/factory utilities).
- Universal `Dpch` envelope and serialization.
- UDP transport stack:
  - `FairLossLink`
  - `StubbornLink`
  - `PerfectLink`
  - `HandshakedPerfectLink`
- Client and server entrypoints wired to the transport stack.
- Unit and integration test infrastructure.

Not implemented yet:
- Basic HotStuff consensus protocol logic.
- Full append-only blockchain execution semantics.

## System Assumptions
- Static membership (`n`, `f`, and replica set fixed before startup).
- PKI material is provisioned before execution.
- Client library is trusted by the application.
- A subset of replicas may be Byzantine.
- UDP is the baseline transport; no TLS channels for Stage 1.

## Communication Stack
```text
+--------------------------------------------------+
| Application / Consensus Logic                    |
+--------------------------------------------------+
| APL (Authenticated Perfect Links) - planned      |
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
  config.yaml
  keys/                         # expected key hierarchy (not fully versioned)
src/
  client/pt/ulisboa/depchain/client/
  server/pt/ulisboa/depchain/server/
  shared/pt/ulisboa/depchain/shared/
    config/
    network/
      dpch/
      links/
        fairloss/
        stubborn/
        perfect/
        handshaked/
  test/java/pt/ulisboa/depchain/
docs/
  project.pdf
  hot-stuff-paper.pdf
pom.xml
```

Maven source mapping:
- `main`: `src/server`, `src/client`, `src/shared`
- `test`: `src/test/java`

## Prerequisites
- Java 21
- Maven 3.9+

Check Java:
```powershell
java -version
```

## Configuration
Main runtime configuration is in `config/config.yaml`, including:
- system parameters (`n`, `f`, leader election, base view),
- replica endpoints and key paths,
- client settings and request timeout,
- timeout values.

Link-layer networking parameters are defined in code defaults:
- `StubbornLink.DEFAULT_*` (retry policy),
- `PerfectLink.DEFAULT_*` (buffering/cleanup),
- `FairLossLink.DEFAULT_MAX_PACKET_SIZE` (network packet size).

Before running:
- ensure key files exist at configured paths,
- ensure configured ports are free,
- keep config consistent across all replicas.

## Build and Test
Build everything:
```powershell
mvn clean package
```

Run unit tests (excludes `integration` tag):
```powershell
mvn test
```

Run integration tests (includes only `integration` tag):
```powershell
mvn verify
```

## Run Locally
Compile classes:
```powershell
mvn -DskipTests package
```

Run one server replica:
```powershell
java -cp target/classes pt.ulisboa.depchain.server.Main server1 config/config.yaml
```

Run another replica:
```powershell
java -cp target/classes pt.ulisboa.depchain.server.Main server2 config/config.yaml
```

Run client:
```powershell
java -cp target/classes pt.ulisboa.depchain.client.Main "hello" server1 config/config.yaml
```

Client usage:
```text
Main <value> <targetReplicaId> <configPath>
```

Server usage:
```text
Main <serverId> <configPath>
```

## Suggested Next Steps
1. Implement Basic HotStuff happy path (`prepare -> pre-commit -> commit -> decide`).
2. Add timeout-driven view change behavior.
3. Add Byzantine protections (signature/QC checks, equivocation handling).
4. Integrate append-only state machine execution with client acknowledgements.
5. Expand fault-injection and adversarial integration tests.

## References
1. HotStuff paper: <https://arxiv.org/pdf/1803.05069>
2. Springer LNCS guidelines: <https://www.springer.com/gp/computer-science/lncs/conference-proceedings-guidelines>
3. Threshold signatures (optional): <https://github.com/weavechain/threshold-sig>
