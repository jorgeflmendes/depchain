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

## 5. Technical Stack

- Language: **Java**.
- Crypto: Java Crypto API.
- Threshold signatures: `weavechain/threshold-sig` is suggested (alternatives are acceptable).
- Design: modular layered abstractions, evaluated for correctness and practical efficiency.

## 6. Repository Status and Layout

Current status:

- project structure is created;
- `build.gradle` is configured with Java 21 toolchain and source sets;
- membership configuration exists in `config/replicas.yaml`;
- key directory structure exists in `config/keys/` (key files are not currently versioned);
- Java implementation under `src/` is still pending.

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
|   |-- replicas.yaml
|   `-- keys/
`-- src/
    |-- client/
    |-- server/
    |-- shared/
    `-- test/
```

## 7. Prerequisites

- Java 21
- Gradle

Note: this repository currently does not include a `gradlew` wrapper script.

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

Optional: generate Gradle wrapper once for team consistency:

```powershell
gradle wrapper
```

Build and test:

```powershell
gradle clean build
gradle test
```

If wrapper exists after generation:

```powershell
.\gradlew.bat clean build
.\gradlew.bat test
```

## 9. Membership Configuration

`config/replicas.yaml` currently defines:

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
