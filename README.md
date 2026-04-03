# DepChain

DepChain is a permissioned blockchain project for the Highly Dependable Systems course.
## Prerequisites

- Java 21 or newer
- Maven 3.6.3 or newer
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

From a clean checkout, compile the project before running replicas or clients:

```powershell
mvn compile
```

The `exec:java` Maven entrypoints use the compiled classes in `target/classes`.

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

Examples:

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


## Testing

The Maven test setup behaves as follows:

- `mvn test` runs the curated non-integration subset,
- `mvn verify` runs that same Surefire phase plus the curated integration subset tagged `integration`.

Test commands:

```powershell
mvn test
mvn verify
mvn clean test
```

The test suite covers:

- ERC-20 `allowance` front-running mitigation,
- invalid quorum certificates emitted by a Byzantine leader,
- leader equivocation, partial broadcast, and post-attack convergence,
- Byzantine replica liveness across multiple attack modes,
- forged client success/failure replies by malicious replicas or leaders,
- replayed client requests and forged client signatures,
- invalid HMAC packets and replayed authenticated nonces,
- exactly-once execution under duplicated and reordered network traffic,
- leader recovery after local persistence failure.

Tests:

1. `QueryAndContractIntegrationTest.approvalFrontRunningScenarioHoldsAtClusterLevel`
2. `ERC20FrontrunningTest.ApprovalTransitions.directNonZeroToNonZeroAllowanceChangeIsRejected`
3. `ERC20FrontrunningTest.ZeroResetMitigation.zeroFirstApprovalFlowPreventsDoubleWithdrawalAcrossFrontRunOrdering`
4. `ByzantineLeaderIntegrationTest.invalidPrepareProposalQcTest`
5. `ByzantineLeaderIntegrationTest.invalidPreCommitQcTest`
6. `ByzantineLeaderIntegrationTest.invalidCommitQcTest`
7. `ByzantineLeaderIntegrationTest.invalidDecideQcTest`
8. `ByzantineLeaderIntegrationTest.equivocatingPrepareProposalTest`
9. `ByzantineLeaderIntegrationTest.partialPrepareBroadcastTest`
10. `ByzantineLeaderIntegrationTest.equivocatingLeaderStillAllowsSubsequentProgressAndHonestConvergence`
11. `ByzantineReplicaIntegrationTest.replicaAttackDoesNotBreakClusterLiveness`
12. `MaliciousClientIntegrationTest.colludingByzantineReplicaCannotForgeClientSuccessWithoutHonestReplyQuorum`
13. `MaliciousClientIntegrationTest.colludingByzantineLeaderCannotForgeClientSuccessWithoutHonestReplyQuorum`
14. `MaliciousClientIntegrationTest.colludingByzantineReplicaCannotForceClientFailureWithoutHonestFailureQuorum`
15. `MaliciousClientIntegrationTest.colludingByzantineLeaderCannotForceClientFailureWithoutHonestFailureQuorum`
16. `HonestClusterIntegrationTest.HappyPath.replayedClientRequestTest`
17. `HonestClusterIntegrationTest.HappyPath.forgedClientSignatureTest`
18. `AuthenticatedIngressIntegrationTest.invalidHmacPacketIsDroppedWithoutBreakingSubsequentClientTraffic`
19. `AuthenticatedIngressIntegrationTest.replayedAuthenticatedNonceIsDroppedWithoutBreakingSubsequentClientTraffic`
20. `AdversarialNetworkClusterIntegrationTest.ExactlyOnceEffects.duplicatedTransportTrafficDoesNotCauseDoubleExecution`
21. `PersistenceFailureIntegrationTest.leaderCanReplyBeforeLocalPersistenceAndCatchUpAfterRestart`

## Notes

- On Windows, Hyperledger Besu may emit a warning about `gnark_eip_196.dll`. The warning is expected in this setup and does not by itself indicate a failed run.
