# DepChain (Highly Dependable Systems)

## Overview
DepChain is a permissioned blockchain project for the Highly Dependable Systems course.

The current codebase includes:
- authenticated client and replica communication,
- HotStuff-based replica consensus,
- native `DepCoin` transfers plus `IST Coin` contract-backed transfers,
- persisted per-replica block history with block-hash chaining and validation.

## Prerequisites
- Java 21
- Maven 3.9+

## Configuration
Main runtime configuration is in `config/config.yaml`.
The authoritative genesis file is in `config/genesis.json`, and replicas/clients load it directly at runtime.
The YAML config includes:
- system parameters (`system.n`, `system.f`),
- replica ids as YAML keys under `replicas`,
- replica network fields grouped under `ports`,
- client identities and connectivity fields (`clients.<id>.senderId`, `clients.<id>.host`, `clients.<id>.knownReplicas`),
- client request timeout (`clients.<id>.requestTimeoutMs`, where `0` means no timeout),
- timeouts (`timeouts.viewChangeMs`, `timeouts.clientCommandWaitMs`, `timeouts.thresholdRoundMs`, `timeouts.fetchNodeMs`),
- runtime key root (`keys.root`), from which replica/client key paths are derived,
- runtime block storage root (`storage.blocksRoot`), from which per-replica block directories are derived.

With the default config:
- key material is generated under `runtime/keys`,
- persisted blocks are stored under `runtime/storage/<replicaId>/blocks`.

## Runtime Validation Rules
Every replica validates client requests before accepting them into consensus:
- the request signature must match the declared client sender id,
- the sender must have enough balance to pay `amount + maxGasCost`,
- the transaction nonce must match the sender account nonce at execution time.

Persisted blocks are also validated structurally:
- each block stores `block_hash` and `previous_block_hash`,
- `block_hash` is derived from `previous_block_hash + gas_used + block transactions`,
- the sum of transaction `gas_limit` values must not exceed the hardcoded block gas limit,
- transactions from the same sender must use consecutive nonces inside a block,
- `gas_used` must be coherent with the block contents.

The current hardcoded block gas limit is `30_000_000`.

## Run Locally
Always run `populate` before starting replicas or clients locally, so the configured key files, threshold material, and `addresses.json` exist under the selected config directory.

Before running:
- ensure key files exist at configured paths,
- ensure configured ports are free,
- keep config consistent across all replicas.

Populate usage:
```text
populate [-c <configPath>]
populate [configPath]
```

`Populate` does not modify `genesis.json`. It only generates key material and an `addresses.json` file with the derived wallet addresses so the genesis can be edited manually.

Maven:
```powershell
mvn exec:java@populate
mvn exec:java@populate "-Dexec.args=config/config.yaml"
```

Client entrypoint usage:
```text
client --config <configPath> --client-id <clientId>
client <configPath> --client-id <clientId>
```

Maven:
```powershell
mvn exec:java@client "-Dexec.args=--config config/config.yaml --client-id client"
mvn exec:java@client "-Dexec.args=--config config/config.yaml --client-id client2"
```

Open a local cluster in separate PowerShell windows:
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\open-local-cluster.ps1 -ProjectDir . -ClientId client
powershell -ExecutionPolicy Bypass -File .\scripts\open-local-cluster.ps1 -ProjectDir . -ClientId client2
```

Open a local cluster on Linux in separate terminal windows:
```bash
./scripts/open-local-cluster.sh --project-dir . --client-id client
./scripts/open-local-cluster.sh --project-dir . --client-id client2
```

Open only the local replicas:
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\open-local-replicas.ps1 -ProjectDir .
```

```bash
./scripts/open-local-replicas.sh --project-dir .
```

Open a local cluster through Maven, selecting the Windows or Linux script automatically:
```powershell
mvn exec:exec@open-local-cluster "-Dcluster.client.id=client"
mvn exec:exec@open-local-cluster "-Dcluster.client.id=client2"
```

Open only the local replicas through Maven:
```powershell
mvn exec:exec@open-local-replicas
```

Client shell commands:
```text
depcoin-transfer <to> <amount> <nonce> [gasLimit] [gasPrice]
depcoin-balance [owner]
my-address
ist-balance [owner]
contract-call <to> <inputHex> <nonce> [amount] [gasLimit] [gasPrice]
ist-transfer <to> <rawValue> <nonce> [gasLimit] [gasPrice]
```

Examples:
```text
depcoin-transfer 3333333333333333333333333333333333333333 25 0
depcoin-balance
my-address
ist-balance 1111111111111111111111111111111111111111
contract-call 1234567890abcdef1234567890abcdef12345678 a9059cbb000000000000000000000000333333333333333333333333333333333333333300000000000000000000000000000000000000000000000000000000000001f4 2
ist-transfer 3333333333333333333333333333333333333333 500 1
```

The client now exposes both assets required by the project:
- `depcoin-transfer` sends native `DepCoin` between EOAs.
- `depcoin-balance` reads native `DepCoin` balances.
- `ist-balance` and `ist-transfer` operate on the `IST Coin` ERC-20 contract.

The replicas translate `IST Coin` requests into the corresponding contract calls internally. The client only accepts a response when a coherent quorum of replicas returns the same value. If replicas answer but no majority agrees, the shell prints an explicit error.

Server entrypoint usage:
```text
server --replica-id <serverId> --config <configPath>
server <serverId> <configPath>
```

Maven:
```powershell
mvn exec:java@server "-Dexec.args=server1 config/config.yaml"
mvn exec:java@server "-Dexec.args=server2 config/config.yaml"
mvn exec:java@server "-Dexec.args=server3 config/config.yaml"
mvn exec:java@server "-Dexec.args=server4 config/config.yaml"
```

## Integration Tests
The integration tests run `populate` internally for each scenario, so it does not need to be executed manually before `mvn verify`.
They are also slow and timing-sensitive, so it may be necessary to increase some test timeouts depending on the machine.
They are designed to succeed with the default `config/config.yaml`.

Maven:
```powershell
mvn verify "-Dit.test=SecTest"
```

Recommended full validation:
```powershell
mvn clean verify
```

The current suite covers:
- normal execution through the initial leader (`server1` accepts 4 valid requests: proves the happy path works repeatedly),
- forwarding from a non-leader replica (`server2` forwards to the leader and the request still succeeds: proves gateway-to-leader forwarding),
- replay of the same signed client request (first delivery succeeds, 10 replays are ignored: proves deduplication by signed request id),
- forged client signatures (no reply is produced; proves invalid client signatures are rejected),
- invalid transaction nonce (request reaches the replicas but execution fails with an explicit error response),
- insufficient balance for `amount + gas` (request reaches the replicas but execution fails with an explicit error response),
- persisted block hash derivation and block-chain linkage,
- per-block gas-limit and per-sender nonce validation in block persistence,
- one Byzantine replica sending an invalid vote (`server3` sends an invalid `PREPARE` vote and the request still succeeds: proves one invalid voter does not prevent progress),
- two Byzantine replicas sending invalid votes (`server3` and `server4` send invalid `PREPARE` votes and the client times out: proves the system cannot form a quorum once the number of dishonest voters exceeds the tolerated threshold).

## Notes
- On Windows, Hyperledger Besu may print a warning about the native library `gnark_eip_196.dll`. In the current setup this does not prevent `mvn clean verify` from succeeding.
