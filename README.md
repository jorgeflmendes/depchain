# DepChain (Highly Dependable Systems)

## Overview
DepChain is a permissioned blockchain project for the Highly Dependable Systems course.

## Prerequisites
- Java 21
- Maven 3.9+

## Configuration
Main runtime configuration is in `config/config.yaml`, and the genesis block template is in `config/genesis.json`.
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


## Run Locally
Always run `Populate` before starting replicas or clients locally, so the configured key files and threshold material exist under `runtime/keys`.

Before running:
- ensure key files exist at configured paths,
- ensure configured ports are free,
- keep config consistent across all replicas.

Populate usage:
```text
Populate [configPath]
```

Maven:
```powershell
mvn exec:java@populate
mvn exec:java@populate "-Dexec.args=config/config.yaml"
```

Client entrypoint usage:
```text
Main --config <configPath> --client-id <clientId>
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
Main <serverId> <configPath>
```

Maven:
```powershell
mvn exec:java@server "-Dexec.args=server1 config/config.yaml"
mvn exec:java@server "-Dexec.args=server2 config/config.yaml"
mvn exec:java@server "-Dexec.args=server3 config/config.yaml"
mvn exec:java@server "-Dexec.args=server4 config/config.yaml"
```

## Integration Tests
The integration tests run `Populate` internally for each scenario, so it does not need to be executed manually before `mvn verify`.
They are also slow and timing-sensitive, so it may be necessary to increase some test timeouts depending on the machine.
They are designed to succeed with the default `config/config.yaml`.

Maven:
```powershell
mvn verify "-Dit.test=SecTest"
```

The current suite covers:
- normal execution through the initial leader (`server1` accepts 4 valid requests: proves the happy path works repeatedly),
- forwarding from a non-leader replica (`server2` forwards to the leader and the request still succeeds: proves gateway-to-leader forwarding),
- replay of the same signed client request (first delivery succeeds, 10 replays are ignored: proves deduplication by signed request id),
- forged client signatures (no reply is produced; proves invalid client signatures are rejected),
- one Byzantine replica sending an invalid vote (`server3` sends an invalid `PREPARE` vote and the request still succeeds: proves one invalid voter does not prevent progress),
- two Byzantine replicas sending invalid votes (`server3` and `server4` send invalid `PREPARE` votes and the client times out: proves the system cannot form a quorum once the number of dishonest voters exceeds the tolerated threshold).
