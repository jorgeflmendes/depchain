# DepChain (Highly Dependable Systems)

## Overview
DepChain is a permissioned blockchain project for the Highly Dependable Systems course.

## Prerequisites
- Java 21
- Maven 3.9+

## Configuration
Main runtime configuration is in `config/config.properties`, including:
- system parameters (`system.n`, `system.f`),
- replica ids, sender ids, hosts, consensus ports, and client ports,
- replica static key paths (`publicKeyPath`, `privateKeyPath`),
- replica threshold key material paths (`thresholdPublicKeyPath`, `thresholdPrivateSharePath`),
- client identity and connectivity fields (`client.id`, `client.senderId`, `client.host`, `client.knownReplicas`),
- client key paths (`client.publicKeyPath`, `client.privateKeyPath`),
- client request timeout (`client.requestTimeoutMs`, where `0` means no timeout),
- view change timeout (`timeouts.viewChangeMs`).


## Run Locally
Always run `Populate` before starting replicas or clients locally, so the configured key files and threshold material exist.

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
mvn exec:java@populate "-Dexec.args=config/config.properties"
```

Client entrypoint usage:
```text
Main <targetReplicaId> <configPath>
```

Maven:
```powershell
mvn exec:java@client "-Dexec.args=server1 config/config.properties"
mvn exec:java@client "-Dexec.args=server2 config/config.properties"
```

Server entrypoint usage:
```text
Main <serverId> <configPath>
```

Maven:
```powershell
mvn exec:java@server "-Dexec.args=server1 config/config.properties"
mvn exec:java@server "-Dexec.args=server2 config/config.properties"
mvn exec:java@server "-Dexec.args=server3 config/config.properties"
mvn exec:java@server "-Dexec.args=server4 config/config.properties"
```

## Integration Tests
The integration tests run `Populate` internally for each scenario, so it does not need to be executed manually before `mvn verify`.
They are also slow and timing-sensitive, so it may be necessary to increase some test timeouts depending on the machine.
They are designed to succeed with the default `config/config.properties`.

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
