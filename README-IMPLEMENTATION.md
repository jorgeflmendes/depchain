# Stage 2 Progress Summary (March 19, 2026)

This document summarizes what was implemented today, starting from the first requested item:

> Add Block & AccountState; extend Node fields.

## 1. Consensus/Proto Foundation

### 1.1 Add Block and AccountState in consensus proto
File: `src/main/proto/consensus.proto`

What was done:
- Added `AccountState` with fields:
  - `address`
  - `balance`
  - `nonce`
- Added CEL validation for `AccountState.address`:
  - lowercase hex, 40 chars (`^[0-9a-f]{40}$`).
- Added `Block` with fields:
  - `height`
  - `block_hash`
  - `previous_block_hash`
  - `transactions`
  - `state`
  - `gas_used`
- Added CEL validations for `Block`:
  - hash shape for `block_hash`
  - hash/null shape for `previous_block_hash`
  - genesis rule: `height == 0` implies previous hash is null/empty
  - non-genesis rule: `height > 0` requires previous hash.

Why:
- This establishes a typed block model and world-state model required by Stage 2.
- CEL rules reject malformed block/account data early.

### 1.2 Remove unused proto import
File: `src/main/proto/consensus.proto`

What was done:
- Removed unused `common.proto` import.

Why:
- Keeps proto clean and avoids warnings.

## 2. Genesis Configuration and Validation

### 2.1 Genesis JSON baseline
File: `src/main/resources/genesis/genesis.json`

What was done:
- Normalized genesis block fields and structure.
- Set `previous_block_hash` to `null` for genesis.
- Kept state map structure (`address -> {balance, nonce, code, storage}`).
- Kept initial transaction list in genesis format.

Why:
- Stage 2 requires a genesis file with block 0 world state and initial transactions.

### 2.2 Genesis parser and validation
File: `src/main/java/pt/ulisboa/depchain/shared/config/GenesisParser.java`

What was done:
- Added `GenesisParser` with JSON loading methods:
  - `load(Path)`
  - `loadDefaultResource()`
- Added structural and semantic validations:
  - `height == 0`
  - `previous_block_hash == null`
  - hash/address format checks
  - non-negative decimal checks for balance/amount
  - tx type checks (`TRANSFER`, `CONTRACT_CALL`, `CONTRACT_DEPLOY`)
  - tx field constraints (`to` presence rules, gas positivity, input hex prefix).

Why:
- Gives a single validated entrypoint for genesis data before runtime execution.

## 3. Runtime Integration

### 3.1 Load genesis during replica startup
File: `src/main/java/pt/ulisboa/depchain/server/runtime/ReplicaServer.java`

What was done:
- Integrated `GenesisParser.loadDefaultResource()` in server boot.
- Added startup logging with genesis metadata.

Why:
- Ensures every replica validates and uses the same genesis snapshot at boot.

### 3.2 Hydrate EVM world state from genesis accounts
File: `src/main/java/pt/ulisboa/depchain/server/runtime/ReplicaServer.java`

What was done:
- Created EVM accounts from genesis `state` entries.
- Applied `balance`, `nonce`, and optional account `code`.

Why:
- Initializes EVM world state before consensus/client transaction handling.

### 3.3 Execute genesis transactions on startup
File: `src/main/java/pt/ulisboa/depchain/server/runtime/ReplicaServer.java`

What was done:
- Added startup execution pipeline for genesis transactions:
  - `TRANSFER` -> `transferNative`
  - `CONTRACT_CALL` -> `callContract`
  - `CONTRACT_DEPLOY` -> `deployContract`
- Added fail-fast behavior with clear tx index in error messages.

Why:
- Completes genesis initialization by applying initial tx effects, not only static state.

## 4. Gas Mechanism Implementation

### 4.1 Gas fee formula in EVM service
File: `src/main/java/pt/ulisboa/depchain/server/evm/EvmService.java`

What was done:
- Implemented fee calculation helper:
  - `fee = gas_price * min(gas_limit, gas_used)`
- Added required-balance checks:
  - sender must have enough for `amount + maxFee`.
- Updated transfer path:
  - success: deduct `amount + fee`
  - insufficient gas limit: fail, charge fee for used gas, increment nonce.
- Updated contract call path:
  - charge fee based on actual gas used
  - increment nonce on completion.

Why:
- Aligns transaction charging with Stage 2 gas requirements.

### 4.2 EVM tests updated
File: `src/test/java/pt/ulisboa/depchain/server/evm/EvmServiceTest.java`

What was done:
- Updated balance assertions to include gas fees.
- Added test for insufficient gas limit in transfer that still charges gas and increments nonce.

Why:
- Verifies correctness and prevents regressions in gas accounting behavior.

## 5. Build and Validation Notes

What was validated during implementation:
- Proto and Java compile checks were run multiple times.
- Focused tests for EVM behavior passed after gas updates.

Environment note encountered:
- On Windows, `protoc` had issues with non-ASCII path segments.
- Workaround used during development: operate through an ASCII path (`C:/dev/depchain-project`).

## 6. Current Status vs Stage 2 Goal

Goal statement:
- Implement genesis block, transactions, gas mechanism, transaction execution, appending and persisting blocks.

Status now:
- Done:
  - Genesis parsing/validation
  - Genesis state hydration
  - Genesis transaction execution
  - Core gas fee mechanism in EVM service
  - Transaction execution service for transfer/call/deploy
- In progress / next:
  - Append blocks from executed transactions
  - Persist blocks in genesis-compatible format
  - Recovery/load persisted blocks on startup
  - Integrate full execution-to-block pipeline in consensus commit path.

## 7. Recommended Next Steps

1. Implement block append service (build block N with previous hash link).
2. Implement block persistence service (write/read JSON block files).
3. Hook persistence into HotStuff commit/decide path.
4. Add tests for:
   - block linking
   - persistence round-trip
   - restart recovery from persisted chain.
