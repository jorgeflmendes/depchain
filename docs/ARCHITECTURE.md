# DepChain Architecture

This document describes the maintained implementation boundaries for the
permissioned Byzantine fault-tolerant blockchain prototype.

## System Context

```mermaid
flowchart LR
    CLIENT["Client CLI / API"] -->|"signed commands"| REPLICA["Replica cluster"]
    REPLICA -->|"ordered blocks"| EVM["EVM execution"]
    EVM --> STATE["Persistent blockchain state"]
    CONFIG["YAML configuration<br/>genesis and keys"] --> CLIENT
    CONFIG --> REPLICA
    TESTS["Integration tests<br/>honest and Byzantine modes"] --> CLIENT
    TESTS --> REPLICA
```

Clients and replicas are configured from the same cluster and genesis material.
The implementation is intended for local experiments and integration testing,
not production key management or open-network deployment.

## Request Path

```mermaid
sequenceDiagram
    participant Client
    participant Ingress as Replica ingress
    participant Consensus as Consensus manager
    participant Quorum as Quorum accumulator
    participant EVM as EVM service
    participant Store as Block persistence

    Client->>Ingress: signed client request
    Ingress->>Ingress: authenticate and reject replays
    Ingress->>Consensus: enqueue proposal input
    Consensus->>Quorum: collect votes and threshold material
    Quorum-->>Consensus: quorum certificate
    Consensus->>EVM: execute ordered block
    EVM-->>Consensus: deterministic result
    Consensus->>Store: persist committed block
    Consensus-->>Client: authenticated response
```

The consensus path owns ordering. Execution is deterministic after a block is
ordered, and persistence records committed blocks so recovery behavior can be
tested independently from the networking layer.

## Network Stack

```mermaid
flowchart TB
    API["Replica / client protocol code"] --> AUTH["AuthenticatedLink"]
    AUTH --> PERFECT["PerfectLink"]
    PERFECT --> STUBBORN["StubbornLink"]
    STUBBORN --> FAIR["FairLossLink"]
    FAIR --> TRANSPORT["Network transport"]
```

The lower layers model progressively stronger communication semantics. Protocol
logic depends on the authenticated boundary instead of directly trusting raw
transport messages.

## Component Responsibilities

| Component | Responsibility | Boundary |
| --- | --- | --- |
| `client` | Builds signed commands and validates responses | Does not order requests |
| `server/consensus` | Leader flow, votes, quorum certificates, and threshold exchange | Does not implement EVM semantics |
| `shared/network` | Fair-loss, stubborn, perfect, and authenticated links | Does not decide protocol validity |
| `server/execution` | Solidity contract loading and EVM execution | Assumes ordered input |
| `server/node` | Replica lifecycle, genesis materialization, and block storage | Owns startup and recovery concerns |
| `shared/config` | Cluster, key, and genesis validation | Rejects invalid deployment assumptions early |

## Adversarial Test Harness

```mermaid
flowchart LR
    SCENARIO["Integration scenario"] --> CLUSTER["Local replica cluster"]
    CLUSTER --> HONEST["Honest replicas"]
    CLUSTER --> BYZ["Byzantine behavior<br/>forged, delayed, duplicated, or equivocated messages"]
    HONEST --> ASSERT["Protocol assertions"]
    BYZ --> ASSERT
```

The tests exercise failure modes such as invalid quorum certificates,
equivocation, forged responses, duplicated traffic, replayed requests, and
persistence recovery. These scenarios define the repository's current quality
bar more accurately than production deployment guarantees.

## Architectural Constraints

- The implementation targets a fixed, configured permissioned cluster.
- Membership changes, operational key rotation, and production monitoring are
  outside the maintained boundary.
- Byzantine behavior is represented through local integration scenarios rather
  than a hostile public network.
- EVM execution is kept behind the ordered-block boundary; consensus does not
  inspect contract internals.
