# DepChain

DepChain is a permissioned blockchain project for the Highly Dependable Systems course.

## Overview

The project implements:

- authenticated client-to-replica and replica-to-replica communication over UDP,
- HotStuff-style replicated consensus with threshold signatures,
- Fair Loss, Stubborn, Perfect, and Authenticated link abstractions,
- native `DepCoin` transfers,
- ERC-20-style `IST Coin` support backed by the embedded EVM runtime,
- persisted per-replica block history with structural validation and recovery,
- genesis materialization based on the configured client keys,
- a split test suite with unit, integration, Byzantine, persistence, networking, and EVM coverage.

## Prerequisites

- Java 21 or newer
- Maven 3.6.3 or newer

The project enforces both versions through the Maven Enforcer plugin.

## Main Components

- `src/main/java/pt/ulisboa/depchain/client`: interactive client shell, request signing, and coherent-reply collection.
- `src/main/java/pt/ulisboa/depchain/server`: replica bootstrap, HotStuff logic, networking APIs, persistence, and execution.
- `src/main/java/pt/ulisboa/depchain/shared`: config parsing, crypto helpers, validation, quorum handling, and link abstractions.
- `src/main/contracts/ISTCoin.sol`: `IST Coin` contract source.
- `config/config.yaml`: runtime topology and paths.
- `config/genesis.json`: genesis template.
- `scripts/`: helper launch scripts for Windows and Linux.

## Configuration

Runtime configuration lives in `config/config.yaml`.

The YAML file defines:

- `system.n` and `system.f`,
- replicas under `replicas.<id>`,
- replica consensus and client ports under `replicas.<id>.ports`,
- clients under `clients.<id>`,
- client `senderId`, `host`, `requestTimeoutMs`, and `knownReplicas`,
- timeout values under `timeouts`,
- the key root under `keys.root`,
- the block storage root under `storage.blocksRoot`.

With the default configuration:

- the cluster runs with `n = 4` and `f = 1`,
- the configured replicas are `server1` through `server4`,
- the configured clients are `client`, `client2`, and `client3`,
- generated keys are stored under `runtime/keys`,
- persisted blocks are stored under `runtime/storage/<replicaId>/blocks`.

`ConfigParser` also validates the configuration for:

- `n >= 3f + 1`,
- unique replica ids, client ids, sender ids, and replica endpoints,
- known replica references inside every client's `knownReplicas`,
- valid and non-empty key/storage paths.

## Genesis and Generated Artifacts

The project follows a two-step genesis flow:

1. `config/genesis.json` is the template committed to the repository.
2. `config/genesis.lock.json` is materialized on first replica startup and becomes the runtime genesis source for that config directory.

`config/genesis.json` is the editable project source of truth.
`config/genesis.lock.json` is a generated runtime artifact that captures the materialized genesis for a specific config directory and key set.
If the lock file is present, replicas load it instead of rematerializing from `genesis.json`.
As a result, genesis changes should be made in `config/genesis.json`, not by manually editing `config/genesis.lock.json`.

During materialization:

- configured client public keys are converted into on-chain wallet addresses,
- missing client accounts are added to the genesis state as EOAs,
- bootstrap native transfers are rewritten to the configured client addresses in client order,
- the initial `IST Coin` bootstrap transfer is rewritten to the first configured client address.

`populate` generates:

- replica public/private key pairs,
- replica threshold public/share material,
- client public/private key pairs,
- `config/addresses.json` with derived client and replica wallet addresses.

`populate` does not create `genesis.lock.json`; that happens when a replica starts.

## Build

From a clean checkout, compile the project before using the runtime helpers:

```powershell
mvn compile
```

The helper scripts and `exec:java` Maven entrypoints use the compiled classes in `target/classes`.

## Run Locally

### 1. Generate key material

Run this before starting replicas or clients:

```powershell
mvn compile exec:java@populate
mvn compile exec:java@populate "-Dexec.args=config/config.yaml"
```

Direct CLI shape:

```text
populate [-c <configPath>]
populate [configPath]
```

### 2. Start replicas

Maven entrypoint:

```powershell
mvn -q exec:java@server "-Dexec.args=server1 config/config.yaml"
mvn -q exec:java@server "-Dexec.args=server2 config/config.yaml"
mvn -q exec:java@server "-Dexec.args=server3 config/config.yaml"
mvn -q exec:java@server "-Dexec.args=server4 config/config.yaml"
```

Direct CLI shape:

```text
server --replica-id <serverId> --config <configPath>
server <serverId> <configPath>
```

### 3. Start a client

Maven entrypoint:

```powershell
mvn -q exec:java@client "-Dexec.args=--config config/config.yaml --client-id client"
mvn -q exec:java@client "-Dexec.args=--config config/config.yaml --client-id client2"
```

Direct CLI shape:

```text
client --config <configPath> --client-id <clientId>
client <configPath> --client-id <clientId>
```

### 4. Use the helper scripts

Windows, full local cluster in separate PowerShell windows:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\open-local-cluster.ps1 -ProjectDir . -ClientId client
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\open-local-cluster.ps1 -ProjectDir . -ClientId client2
```

Linux, full local cluster in separate terminal windows:

```bash
./scripts/open-local-cluster.sh --project-dir . --client-id client
./scripts/open-local-cluster.sh --project-dir . --client-id client2
```

Only the replicas:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\open-local-replicas.ps1 -ProjectDir .
```

```bash
./scripts/open-local-replicas.sh --project-dir .
```

Maven wrappers around those scripts:

```powershell
mvn exec:exec@open-local-cluster "-Dcluster.client.id=client"
mvn exec:exec@open-local-cluster "-Dcluster.client.id=client2"
mvn exec:exec@open-local-replicas
```

The launch scripts invoke the Maven runtime entrypoints directly. Compile the project first and, if a newly installed `mvn` command is not yet visible, restart the terminal so the updated `PATH` is applied.

## Client Shell Commands

The interactive client exposes:

```text
depcoin-transfer <to> <amount> <nonce> [gasLimit] [gasPrice]
depcoin-balance [owner]
ist-transfer <to> <rawValue> <nonce> [gasLimit] [gasPrice]
ist-balance [owner]
contract-call <to> <inputHex> <nonce> [amount] [gasLimit] [gasPrice]
my-address
exit
help
```

Realistic local examples:

- The commands below are intended to be run from the `client` shell on the default local cluster.
- In the default genesis, `client` starts with both `DepCoin` and `IST Coin`; `client2` starts with `DepCoin` only.
- The concrete addresses below match the generated artifacts in this workspace. If your `config/addresses.json` differs, replace the addresses accordingly.

```text
my-address
depcoin-balance
ist-balance
depcoin-transfer 6fc3576405473fcf464839212c701987c5cb6446 25 0
ist-transfer 6fc3576405473fcf464839212c701987c5cb6446 500 1
contract-call f87057cb54f163b94e5ae9206878f1a24394fb08 a9059cbb0000000000000000000000006fc3576405473fcf464839212c701987c5cb644600000000000000000000000000000000000000000000000000000000000001f4 2
```

For reference in the default local setup used above:

- `client` address: `f2d847169048558e56460cda7cb4277a43214a89`
- `client2` address: `6fc3576405473fcf464839212c701987c5cb6446`
- `IST Coin` contract address: `f87057cb54f163b94e5ae9206878f1a24394fb08`

Defaults in the shell:

- `depcoin-transfer` defaults to `gasLimit = 21000` and `gasPrice = 1`,
- `ist-transfer` and `contract-call` default to `gasLimit = 250000` and `gasPrice = 1`.

## Runtime Behavior

### Client-side behavior

- clients broadcast signed requests to every replica listed in `knownReplicas`,
- transaction and query requests are both authenticated,
- the client accepts the first coherent quorum of identical replies,
- the coherent-reply threshold is `f + 1`,
- if replicas reply but no identical quorum can still be formed, the client raises an incoherent-response error.

### Replica-side behavior

- replicas authenticate both client traffic and inter-replica traffic,
- client queries are answered locally without entering consensus,
- client write requests are registered locally and only enqueued by the leader for the current view,
- the leader builds batches up to `32` requests and at most `30_000_000` total gas limit,
- pending requests are ordered by gas price first, then sender id, request id, and nonce,
- oversized requests that cannot fit inside the UDP proposal transport budget are dropped from the pending queue.

### Execution rules

The execution layer enforces:

- request signatures must match the declared client sender id,
- native transfers can target EOAs but not contract accounts,
- contract calls must target deployed contract accounts,
- transaction nonces must match the sender account nonce,
- gas and balance checks are enforced during execution,
- `IST Coin` transfers are translated into contract calls to the configured contract address.

## Persistence

Each replica persists blocks as JSON files under its configured block directory.

The block store validates:

- height monotonicity,
- `previous_block_hash` chaining,
- SHA-256 block hashes,
- per-block gas limit totals,
- `gas_used` coherence,
- consecutive nonces for transactions from the same sender within a block,
- optional HotStuff consensus metadata when present.

The hardcoded maximum block gas limit is:

```text
30_000_000
```

## Testing

The Maven test setup behaves as follows:

- `mvn test` runs Surefire tests and excludes `@Tag("integration")`,
- `mvn verify` runs the same Surefire phase plus Failsafe integration tests tagged `integration`.

Test commands:

```powershell
mvn test
mvn verify
mvn clean test
```

The test suite covers:

- honest-cluster progress and replay rejection,
- forged client signatures and malformed client ingress,
- invalid sender ids, invalid nonces, and insufficient-balance failures,
- Byzantine leaders and Byzantine replicas,
- leader equivocation and forged client responses,
- replica recovery and persistence failure scenarios,
- packet-loss and adversarial network scenarios,
- HotStuff invariants and catch-up logic,
- threshold-signature exchange and protocol behavior,
- Fair Loss, Stubborn, Perfect, and Authenticated link properties,
- client shell parsing,
- config and genesis parsing,
- block persistence validation,
- EVM execution semantics,
- `IST Coin` behavior and approval front-running mitigation,

## Notes

- On Windows, Hyperledger Besu may emit a warning about `gnark_eip_196.dll`. The warning is expected in this setup and does not by itself indicate a failed run.
