# DepChain

[![Tests](https://img.shields.io/github/actions/workflow/status/jorgeflmendes/Highly-Dependable-Systems-26/tests.yml?branch=main&label=tests)](https://github.com/jorgeflmendes/Highly-Dependable-Systems-26/actions/workflows/tests.yml?query=branch%3Amain)
[![Java 21](https://img.shields.io/badge/Java-21-blue)](https://adoptium.net/)
[![Maven](https://img.shields.io/badge/Build-Maven-C71A36)](https://maven.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

DepChain is a permissioned blockchain prototype built for the Highly Dependable Systems course. It implements a Byzantine fault-tolerant replication pipeline with strong focus on safety, liveness under adversarial behavior, and reproducible local experimentation.

## Highlights

- Permissioned replica cluster configured through a single YAML file.
- HotStuff-inspired consensus flow with threshold-cryptography integration.
- Deterministic client request handling with replay protection.
- EVM-based execution path and bundled smart-contract integration.
- Integration test suite covering honest and Byzantine scenarios.

## Tech Stack

- Java 21
- Maven 3.6.3+
- Protocol Buffers
- Hyperledger Besu EVM libraries
- JUnit 5 + Failsafe/Surefire

## Repository Layout

- `src/main/java`: replica, client, consensus, networking, and execution logic.
- `src/main/proto`: protobuf definitions.
- `src/main/contracts`: Solidity contracts used by execution tests and bootstrap.
- `src/test/java`: non-integration and integration test suites.
- `config`: local runtime configuration, genesis templates, and generated addresses.
- `scripts`: convenience scripts for local cluster startup.

## Quick Start

### 1. Prerequisites

- Java 21 or newer
- Maven 3.6.3 or newer

### 2. Build

```powershell
mvn clean compile
```

### 3. Generate key material

```powershell
mvn compile exec:java@populate
```

Optional explicit config path:

```powershell
mvn compile exec:java@populate "-Dexec.args=config/config.yaml"
```

### 4. Start replicas

Run each replica in its own terminal:

```powershell
mvn -q exec:java@server "-Dexec.args=server1 config/config.yaml"
mvn -q exec:java@server "-Dexec.args=server2 config/config.yaml"
mvn -q exec:java@server "-Dexec.args=server3 config/config.yaml"
mvn -q exec:java@server "-Dexec.args=server4 config/config.yaml"
```

### 5. Start a client shell

```powershell
mvn -q exec:java@client "-Dexec.args=--config config/config.yaml --client-id client"
```

## Configuration Model

Runtime configuration is defined in `config/config.yaml`.

Main sections:

- `system`: cluster size (`n`) and tolerated Byzantine faults (`f`).
- `replicas`: replica identities, hosts, and consensus/client ports.
- `clients`: client identity, sender id, and known replica set.
- `timeouts`: view-change, command wait, threshold rounds, and fetch-node settings.
- `keys`: key-material root directory.
- `storage`: block persistence root.

Validation enforces the most important invariants, including `n >= 3f + 1`, unique identities/endpoints, and coherent known-replica references.

## Genesis and Materialization

DepChain uses a two-step genesis process:

1. `config/genesis.json` is the editable project template committed to git.
2. `config/genesis.lock.json` is generated at runtime on first replica startup for that config directory.

When present, `genesis.lock.json` is treated as the runtime source. For predictable behavior, edit `genesis.json` and regenerate runtime artifacts instead of manually editing lock files.

## Client Shell Commands

The interactive client includes:

```text
depcoin-transfer <to> <amount> <nonce> [gasLimit] [gasPrice]
depcoin-balance [owner]
ist-transfer <to> <rawValue> <nonce> [gasLimit] [gasPrice]
ist-balance [owner]
contract-call <to> <inputHex> <nonce> [amount] [gasLimit] [gasPrice]
my-address
help
exit
```

## Testing

- `mvn test`: curated non-integration tests.
- `mvn verify`: non-integration + integration suite (Failsafe).

Useful commands:

```powershell
mvn test
mvn verify
mvn clean verify
```

The integration suite covers, among other topics:

- Byzantine leader invalid QC injection and equivocation.
- Byzantine replica liveness and client-response forgery resistance.
- Replay protection and authenticated-ingress hardening.
- Exactly-once behavior under duplicated/reordered traffic.
- Persistence-failure recovery behavior.

## CI

GitHub Actions workflow: `.github/workflows/tests.yml`.

It runs `mvn -B clean verify` on pushes and pull requests.

## Security

Please report vulnerabilities privately by following the process in [SECURITY.md](SECURITY.md).

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) first.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).

## Notes

- On Windows, Hyperledger Besu may emit a warning related to `gnark_eip_196.dll`. In this project setup, that warning alone does not necessarily indicate failure.
