# DepChain – Client Library (Stage 1)

## Overview

This module implements the **client side of DepChain**.
The client library is responsible for:

1. Sending requests to a blockchain replica.
2. Waiting for a response.
3. Returning the result to the application.

At this stage of the project, the only supported operation is:

```
append(string)
```

The client sends a string to the system, which will eventually be appended to the blockchain state once consensus is reached.

For now, the server used for testing simply echoes the received message.

---

# Client Architecture

The client implementation is composed of the following classes:

```
client/
│
├── Main.java
├── DepChainClient.java
├── ClientRequest.java
├── ClientReply.java
└── ClientCodec.java
```

Each class has a specific responsibility.

---

# Main.java

### Purpose

Entry point for the client application.

### Responsibilities

* Parses command line arguments.
* Instantiates the client library.
* Calls the `append` operation.
* Prints the response received from the system.

### Example execution

```
java -cp target/classes pt.ulisboa.depchain.client.Main hello replica1 config/config.yaml
```

Arguments:

```
Main <value> <targetReplicaId> <configPath>
```

Example:

```
Main hello replica1 config/config.yaml
```

This sends the string `"hello"` to the replica identified as `replica1`.

---

# DepChainClient.java

### Purpose

Core class of the client library.

This class handles all communication between the client and the blockchain replicas.

### Responsibilities

* Load the system configuration.
* Resolve replica addresses from the configuration file.
* Create the network transport layer.
* Send requests to the replica.
* Wait for responses.
* Return the result to the application.

### Main Method

```
append(String value, String replicaId)
```

Steps executed:

1. Resolve the target replica address.
2. Open a `HandshakedPerfectLink`.
3. Serialize the request.
4. Send the request to the replica.
5. Wait for a response with the same `connectionId`.
6. Decode the reply.
7. Return the result.

### Request Flow

```
Client
  |
  | append("hello")
  v
Replica (gateway)
  |
  | forward to leader
  v
Consensus (HotStuff)
  |
  v
Replica
  |
  v
Client receives response
```

---

# ClientRequest.java

### Purpose

Represents a request sent by the client.

### Current structure

```
ClientRequest(String value)
```

This represents the operation:

```
append(value)
```

Example request:

```
append("hello")
```

---

# ClientReply.java

### Purpose

Represents the response received from a replica.

### Current structure

```
ClientReply(String value)
```

The server currently returns a simple message such as:

```
Received hello
```

or later:

```
SUCCESS
```

---

# ClientCodec.java

### Purpose

Handles serialization and deserialization of messages exchanged between client and server.

### Methods

#### encodeRequest

```
byte[] encodeRequest(ClientRequest req)
```

Converts the request into a byte array before sending it through the network.

Current implementation:

```
value → UTF-8 bytes
```

Example:

```
"hello" → [104 101 108 108 111]
```

---

#### decodeReply

```
ClientReply decodeReply(byte[] payload)
```

Converts the received byte array back into a string reply.

Example:

```
[82 101 99 101 105 118 101 100 32 104 101 108 108 111]
→ "Received hello"
```

---

# Network Layer

The client communicates with replicas using the **HandshakedPerfectLink** abstraction.

Communication stack:

```
Application
    │
HandshakedPerfectLink
    │
PerfectLink
    │
StubbornLink
    │
FairLossLink
    │
UDP
```

This stack provides:

* Reliable delivery
* Message ordering
* Duplicate filtering
* Connection handshake

---

# Connection Handling

Each request uses a randomly generated `connectionId`.

```
long connectionId = ThreadLocalRandom.current().nextLong();
```

This identifier ensures that the client matches the response to the correct request.

Example:

```
Client sends request → connectionId = 1837265
Server replies → connectionId = 1837265
```

The client only accepts responses with the matching identifier.

---

# Current Testing Setup

The current server implementation simply echoes the received request.

Example interaction:

Client:

```
append("hello")
```

Server receives:

```
hello
```

Server responds:

```
Received hello
```

Client output:

```
response = Received hello
```

---

## Running and Testing the Client–Server Communication

Before integrating the HotStuff consensus algorithm, the client can already be tested using the current server implementation, which simply echoes back the received message. This allows us to validate the networking stack and request/response flow.

### 1. Compile the Project

From the root directory of the repository, compile the project using Maven:

```id="0n1p7y"
mvn clean package
```

This will compile all source files and generate the `.class` files inside:

```id="r0av6s"
target/classes
```

---

### 2. Start a Replica (Server)

Open a terminal in the project root directory and start a replica node:

```id="i06w0h"
java -cp target/classes pt.ulisboa.depchain.server.Main server1 config/config.yaml
```

Expected output:

```id="q0x0oi"
Replica server1 listening for client UDP requests on 127.0.0.1:<port>
```

This process represents one blockchain replica waiting for client requests.

---

### 3. Send a Client Request

Open another terminal (also in the project root directory) and run the client:

```id="w3yyls"
java -cp target/classes pt.ulisboa.depchain.client.Main hello server1 config/config.yaml
```

Arguments:

```id="9j96u7"
Main <value> <targetReplicaId> <configPath>
```

Example:

```id="n5p8kq"
Main hello server1 config/config.yaml
```

This sends the string `"hello"` to the replica identified as `server1`.

---

### 4. Expected Result

The server receives the request and sends back a response.

Client terminal:

```id="d6r3iy"
response = Received hello
```

This confirms that:

* The client successfully sent the request.
* The replica received the message.
* The response was correctly returned through the network stack.

---


# Future Integration

When the HotStuff consensus algorithm is implemented, the flow will change slightly.

```
Client
  |
  v
Replica (gateway)
  |
  v
Leader
  |
  v
HotStuff consensus
  |
  v
Blockchain state updated
  |
  v
Client receives confirmation
```

The client implementation will remain mostly unchanged.

---

# Summary

The client module currently provides:

* A CLI interface for submitting requests
* Reliable communication with replicas
* Request/response handling
* Message serialization

This implementation allows the team to test the networking stack and prepare for integration with the consensus module.

---
