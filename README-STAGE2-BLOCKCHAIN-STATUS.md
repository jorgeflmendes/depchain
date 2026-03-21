# Stage 2 Blockchain Status and Code Map

## What Is Implemented

### 1) Genesis block concept
- Genesis schema parsing and validation:
  - src/main/java/pt/ulisboa/depchain/shared/config/GenesisParser.java
  - Main entrypoint: loadDefaultResource()
- Genesis load at server startup:
  - src/main/java/pt/ulisboa/depchain/server/runtime/ReplicaServer.java
  - Constructor lines where genesis is loaded and persisted bootstrap is ensured.
- Genesis state and transactions applied to EVM world at startup:
  - ReplicaServer.applyGenesisState(...)
  - ReplicaServer.applyGenesisTransactions(...)
  - ReplicaServer.applyGenesisTransaction(...)

### 2) Transactions concept
- Protobuf transaction contract:
  - src/main/proto/client.proto
  - TransactionRequest, TransactionReceipt, TransactionResponse
- Command wrapping into consensus nodes:
  - src/main/java/pt/ulisboa/depchain/server/consensus/hotstuff/HotStuffManager.java
  - nodeCommandForRequest(...)

### 3) Gas mechanism
- Native transfer gas and fee charging:
  - src/main/java/pt/ulisboa/depchain/server/evm/EvmService.java
  - transferNative(...)
- Contract call gas and fee charging:
  - EvmService.callContract(...)
- Fee computation helper:
  - EvmService.calculateFee(...)

### 4) Transaction execution (including smart contract execution)
- EVM execution engine wrapper:
  - EvmService.execute(...)
- Contract deploy path:
  - EvmService.deployContract(...)
- Contract call path:
  - EvmService.callContract(...)
- HotStuff execution path for transactions and receipts:
  - HotStuffManager.transactionResponse(...)

### 5) Appending and persisting blocks
- Persistence model and append guards:
  - src/main/java/pt/ulisboa/depchain/server/runtime/BlockStore.java
  - ensureGenesisPersisted(...)
  - append(...)
  - loadLatest(...)
- Atomic write path for robustness:
  - BlockStore.writeBlock(...)
- Runtime hook that appends blocks after execution:
  - src/main/java/pt/ulisboa/depchain/server/runtime/ReplicaServer.java
  - onExecutedNode(...)
  - extractPersistedTransactions(...)
  - snapshotKnownAccounts(...)

## Step 4 Confirmation (Consensus connected to execution + append)

### Status: YES
Consensus is connected to execution and block append in the committed path:
- HotStuff decides and commits node order.
- Committed branch is executed in order:
  - HotStuffManager.executeCommittedBranch(...)
  - HotStuffManager.executeCommand(...)
- After execution, runtime persistence hook is invoked:
  - HotStuffManager.onNodeExecuted callback
  - Wired in ReplicaServer constructor with this::onExecutedNode
- The runtime persists each executed committed node as a block:
  - ReplicaServer.onExecutedNode(...)
  - BlockStore.append(...)

This means consensus is effectively responsible for ordering what gets executed and appended.

## Important Implementation Detail
- Gas used is propagated from transaction execution receipt to the Node passed to persistence hook:
  - HotStuffManager.nodeWithObservedGasUsed(...)
- This ensures persisted block gas_used reflects actual execution in transaction commands.

## Test Evidence

### Unit tests
- src/test/java/pt/ulisboa/depchain/server/evm/EvmServiceTest.java
- src/test/java/pt/ulisboa/depchain/server/runtime/BlockStoreTest.java
- src/test/java/pt/ulisboa/depchain/server/consensus/hotstuff/HotStuffCatchUpTest.java

### Integration test for persistence
- src/test/java/pt/ulisboa/depchain/integration/cluster/BlockPersistenceIntegrationTest.java
- Validates:
  - transaction response success
  - persisted transfer transaction exists in block files
  - persisted recipient state is updated
  - persisted gas_used is correct for transfer
  - previous_block_hash links to genesis block hash

## What Is Not Part of This Yet
- Full client shell API for transaction submission (append path exists; transaction CLI flow is still TODO in DpchClient).
- Persisting full receipt object inside BlockDocument (today block stores tx + state + gas_used; receipt is returned to client but not yet persisted as receipt structure).

## Suggested Next Team Steps
1. Persist receipt metadata in block documents (success, return_data, error_message, tx hash).
2. Add client shell commands for transfer and contract call.
3. Add more integration tests for contract-call persistence and multi-block chain continuity under repeated traffic.
