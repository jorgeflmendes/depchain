# Changes Summary - client.proto 

This document summarizes what was added in [proto/client.proto](proto/client.proto).

## Goal

Extend the client API from a single append-only request model to a transaction-capable model, while preserving backward compatibility with existing append flow.

## What Was Added

### 1) New transaction type enum

Added `TransactionKind` with:
- `TRANSACTION_KIND_UNSPECIFIED = 0`
- `TRANSACTION_KIND_TRANSFER = 1`
- `TRANSACTION_KIND_CONTRACT_CALL = 2`

Why:
- Distinguishes transfer operations from contract-call operations.
- Creates a clear typed base for execution and gas rules.

### 2) New `TransactionRequest` message

Added a transaction request with these fields:
- `request_key` (existing dedup key model)
- `kind`
- `to`
- `amount`
- `nonce`
- `gas_limit`
- `gas_price`
- `data`
- `signature`

Why:
- Supports requirements for transactions and gas.
- Keeps replay-resistance and signature verification structure aligned with existing request design.

### 3) New `TransactionReceipt` message

Added receipt fields:
- `transaction_hash`
- `node_hash`
- `success`
- `gas_used`
- `error_message` (optional)
- `return_data` (optional)

Why:
- Defines execution output contract for clients.
- Allows reporting gas usage and failure reason.

### 4) New `TransactionResponse` message

Added response fields:
- `accepted`
- `message`
- `receipt` (optional)

Why:
- Separates acceptance-level response from append semantics.
- Supports asynchronous or failure cases where receipt may not be present.

### 5) Extended oneof wrappers

`ClientRequest` now supports:
- `append = 1`
- `transaction = 2`

`ClientResponse` now supports:
- `append = 1`
- `transaction = 2`

Why:
- Preserves old append path while introducing new transaction path.
- Minimizes migration breakage.

## Compatibility Notes

- Existing append messages were kept.
- Existing field numbers were not changed.
- New transaction support was introduced using new field numbers.

This allows old code to keep working while new transaction-aware code is added incrementally.

## Validation Notes

Current file enforces required fields for core transaction parameters.

Design choice currently in file:
- `data` is allowed to be empty at schema level.

This is useful because:
- transfer transactions may not need payload bytes.
- contract-call-specific payload requirements can be enforced later in execution logic or with CEL constraints.

## What Is Not Implemented Yet

This task only changed the protobuf schema.
The following are still pending in later tasks:
- signature payload updates for transaction fields
- server-side transaction validation/execution
- gas accounting logic
- block metadata propagation from execution
- persistence and recovery integration

## How This Was Verified

The project was compiled and targeted tests were run successfully after Task 1 edits:
- `HotStuffCatchUpTest`
- `ThresholdSignatureProtocolTest`

## Next Step

Proceed to [proto/consensus.proto](proto/consensus.proto):
- add optional execution metadata fields on `Node` (for example gas usage and state root marker)
- keep node hash payload semantics unchanged