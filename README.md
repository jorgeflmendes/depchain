# DepChain (Dependable Chain) - Stage 1

This repository contains the Stage 1 foundation for the Highly Dependable Systems project, focused on the consensus layer of a permissioned blockchain.

## 1. Project Scope and Goals

The goal is to build a permissioned (closed membership), highly dependable blockchain system called **DepChain**.

Stage 1 focuses on:

- consensus using **Basic HotStuff** (Algorithm 2 from the reference paper);
- client to blockchain request/response integration;
- append-only in-memory storage for committed values.

Implementing **Chained HotStuff** (Section 5 of the paper) is optional and can be treated as an extra challenge.

## 2. System Assumptions

- **Static membership**: blockchain members and leader set are fixed before startup.
- **PKI available at startup**: public/private keys are generated and distributed before execution.
- **Threat model**: the local client library is trusted by the application, but a subset of blockchain members may be Byzantine.

## 3. System Architecture

The system is split into:

1. **Blockchain members (replicas/servers)**: maintain state and run HotStuff consensus.
2. **Client library**: embedded in the client application and maps app operations to blockchain requests.

Required interaction:

- client sends `<append, string>`;
- system returns whether/when the request was executed;
- when consensus reaches `DECIDE`, an upcall notifies the upper layer.

The Stage 1 blockchain state is an append-only array/list of strings in memory.

## 4. Networking and Communication Constraints

- Base network behavior is unreliable (loss, delay, duplication, corruption).
- Communication must use **UDP** as transport baseline.
- Secure channels such as TLS are not allowed for this stage.
- Reliability/authentication abstractions should be built on top of UDP (for example, Authenticated Perfect Links).

Networking layering roadmap:

```text
                 /\
                /  \
               / App\             Client + consensus logic
              /------\
             /  APL   \           Authenticated Perfect Links (to implement)
            /----------\
           /   PL       \         Perfect Links semantics (to implement over APL)
          /--------------\
         / UDP FairLoss   \       FairLossLink over UDP (already implemented)
        /------------------\
       /   UDP Datagram     \     Unreliable network baseline
      /______________________\
```

Interpretation:

- The current base is `FairLossLink` in `src/shared/.../links/fairloss/transport`.
- Next, this base should be extended to add authentication (APL).
- On top of authenticated communication, enforce perfect-link guarantees used by upper layers.

## 5. Technical Stack

- Language: **Java**.
- Crypto: Java Crypto API.
- Threshold signatures: `weavechain/threshold-sig` is suggested (alternatives are acceptable).
- Design: modular layered abstractions, evaluated for correctness and practical efficiency.

## 6. Repository Status and Layout

Current status:

- project structure is created and builds with Java 21;
- membership and network configuration is centralized in `config/config.yaml`;
- key directory structure exists in `config/keys/` (key files are not currently versioned);
- a full Fair Loss Links communication baseline over UDP is implemented;
- strict configuration parsing/validation and automated tests are in place.
- Basic HotStuff consensus and append-only blockchain state machine are still pending.

Project layout:

```text
.
|-- build.gradle
|-- settings.gradle
|-- README.md
|-- docs/
|   |-- hot-stuff-paper.pdf
|   `-- project.pdf
|-- config/
|   |-- config.yaml
|   `-- keys/
`-- src/
    |-- client/pt/ulisboa/depchain/client/Main.java
    |-- server/pt/ulisboa/depchain/server/Main.java
    |-- shared/pt/ulisboa/depchain/shared/
    |   |-- config/ConfigFile.java
    |   `-- links/fairloss/
    |       |-- transport/FairLossLink.java
    |       |-- transport/InboundRequest.java
    |       |-- codec/
    |       |   |-- FairLossPacketCodec.java
    |       |   |-- FairLossMessageCodec.java
    |       |   `-- BinaryFieldIO.java
    |       `-- message/
    |           |-- FairLossLinkMessage.java
    |           |-- FairLossRequestMessage.java
    |           `-- FairLossResponseMessage.java
    `-- test/java/pt/ulisboa/depchain/
        |-- integration/ReplicaConnectivityTest.java
        `-- shared/
            |-- config/ConfigFileTest.java
            `-- links/fairloss/
                |-- codec/FairLossPacketCodecTest.java
                |-- codec/BinaryFieldIOTest.java
                `-- transport/FairLossLinkTest.java
```

What each file does:

- `build.gradle`: Gradle setup (plugins, Java 21 toolchain, source sets, test tasks, and run defaults).
- `settings.gradle`: Defines the Gradle root project name (`depchain`).
- `config/config.yaml`: Static system configuration:
  - system parameters (`n`, `f`, leader election, base view);
  - replica endpoints (consensus/client ports and key paths);
  - client settings (known replicas and timeout);
  - network limits (max packet size).
- `config/keys/`: Key material directory structure (files are expected at runtime, but not versioned here).

- `src/client/pt/ulisboa/depchain/client/Main.java`: CLI client entrypoint. Loads config, builds one request, sends it to a target replica, and prints the response.
- `src/server/pt/ulisboa/depchain/server/Main.java`: Replica entrypoint. Loads config, binds UDP socket, receives requests in a loop, and handles them using virtual threads.

- `src/shared/pt/ulisboa/depchain/shared/config/ConfigFile.java`: Strict parser/validator for `config/config.yaml` with consistency checks (ports, replica IDs, thresholds, packet size).

- `src/shared/pt/ulisboa/depchain/shared/links/fairloss/transport/FairLossLink.java`: Low-level UDP request/reply link (fair-loss semantics). Handles send, receive, timeout waiting, and packet decoding.
- `src/shared/pt/ulisboa/depchain/shared/links/fairloss/transport/InboundRequest.java`: Immutable envelope for an inbound request plus sender endpoint metadata (`senderIp`, `senderPort`).

- `src/shared/pt/ulisboa/depchain/shared/links/fairloss/codec/FairLossPacketCodec.java`: Packet framing/unframing (`magic`, `version`, `messageType`) and dispatch to message codecs.
- `src/shared/pt/ulisboa/depchain/shared/links/fairloss/codec/FairLossMessageCodec.java`: Codec interface implemented by each message type.
- `src/shared/pt/ulisboa/depchain/shared/links/fairloss/codec/BinaryFieldIO.java`: Reusable binary read/write helpers for primitive types, UUID, strings, and byte arrays.

- `src/shared/pt/ulisboa/depchain/shared/links/fairloss/message/FairLossLinkMessage.java`: Base message contract (`messageTypeId`).
- `src/shared/pt/ulisboa/depchain/shared/links/fairloss/message/FairLossRequestMessage.java`: Request message model + serialization logic.
- `src/shared/pt/ulisboa/depchain/shared/links/fairloss/message/FairLossResponseMessage.java`: Response message model + serialization logic.

- `src/test/java/pt/ulisboa/depchain/shared/config/ConfigFileTest.java`: Unit tests for config parsing/validation.
- `src/test/java/pt/ulisboa/depchain/shared/links/fairloss/codec/FairLossPacketCodecTest.java`: Unit tests for packet codec round-trip and invalid packet handling.
- `src/test/java/pt/ulisboa/depchain/shared/links/fairloss/codec/BinaryFieldIOTest.java`: Unit tests for primitive/structured binary field IO and invalid length checks.
- `src/test/java/pt/ulisboa/depchain/shared/links/fairloss/transport/FairLossLinkTest.java`: Unit tests for UDP request/reply behavior (including requestId matching).
- `src/test/java/pt/ulisboa/depchain/integration/ReplicaConnectivityTest.java`: Integration test that boots replicas as processes and verifies client connectivity to all of them.

## 7. Prerequisites

- Java 21
- Gradle (optional if using the included wrapper)

This repository includes Gradle wrapper scripts (`gradlew` / `gradlew.bat`).

Gradle source roots are configured as:

- `src/server`
- `src/client`
- `src/shared`
- `src/test/java`

## 8. Local Setup, Build, and Test

Check Java:

```powershell
java -version
```

Build and test:

```powershell
.\gradlew.bat clean build
.\gradlew.bat test
.\gradlew.bat integrationTest
```

Run one replica locally (defaults to `server1` and `config/config.yaml`):

```powershell
.\gradlew.bat run
```

Run a specific replica/config:

```powershell
.\gradlew.bat run -PreplicaId=server2 -PconfigPath=config/config.yaml
```

## 9. Membership Configuration

`config/config.yaml` currently defines:

- `n = 4` replicas, `f = 1`;
- localhost addresses and per-replica consensus/client ports;
- timeout values for view changes and retransmissions.

Before execution, ensure:

- key files exist at the configured paths;
- required ports are free;
- all replicas use the same membership and timeout configuration.

## 10. Recommended Implementation Roadmap

1. Implement shared models and serialization in `src/shared`.
2. Implement UDP networking plus authenticated/reliable link abstraction in `src/server`.
3. Implement Basic HotStuff happy path (`prepare -> pre-commit -> commit -> decide`).
4. Add crash-fault handling (timeouts and view change).
5. Add Byzantine protections (signature checks, QC validation, equivocation detection).
6. Implement client library flow and append service integration.
7. Add intrusive tests in `src/test/java`.

## 11. Testing and Validation Requirements

Black-box tests are not enough. Test infrastructure should support fault injection, including:

- message loss/delay/duplication/manipulation;
- malicious or faulty leader behavior;
- invalid signatures and forged messages;
- conflicting proposals in the same view.

## 12. Submission and Evaluation (From the Project Brief)

- Deadline: **March 10 at 23:59** via Fenix.
- Team ethics: work must be original to the group.
- Required deliverables:
  - ZIP with source code, dependencies, demos, and attack/byzantine simulations;
  - README with explicit reproduction steps for tests and demos;
  - Report (max 5 pages, Springer LNCS format) with:
    - design decisions and justification;
    - threat analysis;
    - protection mechanisms;
    - dependability guarantees.

## References

1. HotStuff paper: <https://arxiv.org/pdf/1803.05069>
2. Springer LNCS guidelines: <https://www.springer.com/gp/computer-science/lncs/conference-proceedings-guidelines>
3. Threshold signatures (optional): <https://github.com/weavechain/threshold-sig>
