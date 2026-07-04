# DepChain

> A permissioned Byzantine fault-tolerant blockchain for studying dependable replication under adversarial behavior.

[![Tests](https://github.com/jorgeflmendes/depchain/actions/workflows/tests.yml/badge.svg)](https://github.com/jorgeflmendes/depchain/actions/workflows/tests.yml)
[![Java 21](https://img.shields.io/badge/Java-21-blue)](https://adoptium.net/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

DepChain is a systems and security project that combines a HotStuff-inspired
consensus flow, authenticated communication, threshold cryptography, persistent
block storage, and EVM-backed transaction execution. Its integration suite
exercises both honest clusters and deliberately Byzantine participants.

## Overview

The system runs a configurable permissioned replica cluster that tolerates up
to `f` Byzantine faults when `n >= 3f + 1`. Clients submit signed requests,
replicas establish consensus on ordered blocks, and deterministic execution
updates EVM-backed state. Replay protection and persistence recovery are tested
as first-class protocol concerns.

This project demonstrates protocol design, defensive validation, cryptographic
message handling, concurrent networking, deterministic execution, and
adversarial integration testing.

## Academic Context

This project was developed as part of the **Highly Dependable Systems** course
unit at **Instituto Superior Técnico, University of Lisbon**.

This project explores dependable distributed systems, fault tolerance,
replication, and blockchain-inspired consistency mechanisms.

## Key Features

- HotStuff-inspired consensus and quorum-certificate validation
- Authenticated client and replica communication
- Threshold-signature exchange and key-material tooling
- Deterministic request processing with replay protection
- EVM execution with bundled Solidity contracts
- Persistent block storage and recovery behavior
- Honest, Byzantine, and adversarial-network integration tests
- YAML-based cluster configuration with invariant validation

## Architecture

```text
Client
  | signed request
  v
Replica ingress -> HotStuff-inspired consensus -> Ordered block
  |                                               |
  | authenticated replica links                   v
  +----------------------------------------> EVM execution
                                                  |
                                                  v
                                           Persistent state
```

The main implementation areas are:

- `client`: request construction, response validation, and interactive shell
- `server/consensus`: leader flow, quorum handling, and threshold signatures
- `shared/network`: fair-loss, stubborn, perfect, and authenticated links
- `server/execution`: EVM execution and contract integration
- `server/node`: replica lifecycle, block storage, and genesis materialization
- `shared/config`: configuration and genesis validation

## Tech Stack

- Java 21 and Maven
- Protocol Buffers
- Hyperledger Besu EVM libraries
- Solidity
- JUnit 5, Surefire, and Failsafe
- GitHub Actions

## Repository Structure

```text
.
|-- config/                 # Cluster and genesis configuration
|-- src/main/contracts/     # Solidity contracts
|-- src/main/java/          # Client, replica, consensus, network, and EVM code
|-- src/main/proto/         # Protocol definitions
|-- src/test/java/          # Unit and integration tests
|-- CONTRIBUTING.md
|-- SECURITY.md
`-- pom.xml
```

## Getting Started

Prerequisites: Java 21+ and Maven 3.6.3+.

```bash
git clone https://github.com/jorgeflmendes/depchain.git
cd depchain
mvn clean compile
mvn compile exec:java@populate
```

Start four replicas in separate terminals:

```bash
mvn -q exec:java@server "-Dexec.args=server1 config/config.yaml"
mvn -q exec:java@server "-Dexec.args=server2 config/config.yaml"
mvn -q exec:java@server "-Dexec.args=server3 config/config.yaml"
mvn -q exec:java@server "-Dexec.args=server4 config/config.yaml"
```

Start the client shell:

```bash
mvn -q exec:java@client "-Dexec.args=--config config/config.yaml --client-id client"
```

Representative commands include `depcoin-transfer`, `depcoin-balance`,
`ist-transfer`, `ist-balance`, `contract-call`, and `my-address`.

## Running Tests

```bash
mvn test
mvn verify
```

`mvn verify` includes integration scenarios for invalid quorum certificates,
equivocation, Byzantine replica behavior, forged responses, duplicated and
reordered traffic, replay protection, and persistence failure recovery.

GitHub Actions runs `mvn -B clean verify` on pushes and pull requests.

## Limitations

- The project is a research and teaching prototype, not a production blockchain.
- The default configuration is intended for local experimentation.
- Operational deployment, membership changes, and production key management
  are outside the current scope.
- Hyperledger Besu may emit a platform-specific native-library warning on
  Windows; the warning alone does not necessarily indicate a failed run.

## Roadmap

- Add a reproducible latency and throughput benchmark harness
- Publish protocol sequence diagrams and a formal fault-model summary
- Add containerized multi-replica startup
- Expose runtime consensus and storage metrics

## License

Licensed under the [MIT License](LICENSE). Security reports should follow
[SECURITY.md](SECURITY.md), and contributions should follow
[CONTRIBUTING.md](CONTRIBUTING.md).
