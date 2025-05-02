# 00_mktl – Unified mKTL Architecture, Terminology, and Protocol Specification

## 1. Introduction

The modern Keck Task Library (**mKTL**) protocol is a flexible, decentralized messaging system designed to manage 
instrument control, status monitoring, and data interchange at the **W. M. Keck Observatory**. It draws conceptual 
lineage from the original KTL system while embracing modern messaging infrastructure — particularly ZeroMQ multipart 
messaging with zyre and zgossip — in a design that supports distributed authority, minimal centralization, and 
efficient extensibility... while maintaining the simplicity to embedd full mKTL compliant daemons in IoT-scale 
embedded devices.


This document defines the conceptual model, message protocols, key definitions, and architectural 
responsibilities that together constitute the mKTL system. It replaces and unifies prior 
documentation in `01_nomenclature`, `02_protocol`, and `03_config`.

## 2. Conceptual Model

At its core, the mKTL protocol enables **key-based communication and control** between distributed processes. 
The architecture supports daemon-driven instrumentation control, GUI tools, monitoring services, automated agents, 
and legacy bridges, all through a shared messaging model.

### 2.1 Keys and Stores

The basic unit of mKTL is a **key**, which represents a control or telemetry channel. Keys are grouped into 
**stores**, each representing a logical subsystem (e.g., `guider`, `adc`, `power`). A full key name is a 
dot-delimited path of the form:

```
<store>.<subsystem>.<key>
```

The **store name** is always the first segment of the path. While a flat model (`store.key`) is supported, 
this format is compatible with **nested stores** and hierarchical namespaces. For example:

```
guider.status.exposure_id
adc.temperature.board
```

Extension to support dynamic authority and routing across such keypaths is possible and could be implemented by routing
or proxy nodes in the network or by a distributed negotiation by making the keys first-class objects. It is not, however,
intrinsic to the on-the-wire IPC protocol level.

### 2.2 Participants and Authority

There are two primary participant roles in mKTL:

- **Clients** issue `get` and `set` requests and can subscribe to keys (which may or may not be published).
- **Daemons** serve authoritative values and configuration for a subset of keys, they issue `ack`, `response`, and `config` messages

The `MKTLComs` facility allows for any program to be simultaneously a **Client** and a **Daemon**, it only need to define a set of keys it 
services for the swarm to be able to route `get` and `set` requests to it. 

A given key is **owned by exactly one daemon**, though provision and planning has been done to allow for shadowing and proxying. 
That daemon must respond to requests and optionally emit `publish` updates. Daemons advertise their key ownership 
to the network, allowing other's `MKTLComs` to resolve key-to-network identity mappings without the need for any 
centralized configuration file.

### 2.3 Values and Types

Key values are JSON-serializable by default. Supported native types include:

- `int`, `float`, `bool`, `str`
- `enum` (named string values mapped to integers)
- `binary` (attached as separate message frame)
- `timestamp`, `status`, `duration`, etc.
- json serialization of enitre first-class objects is possible
Compound and structured values are encouraged. Enums are especially preferred over unstructured strings or 
numeric codes, allowing self-describing protocols:

```json
{
  "value": "in_position",
  "enumerators": {
    "in_position": 1,
    "moving": 2,
    "error": 3
  }
}
```

### 2.4 Operations and Message Types

All mKTL communication takes place over **multipart ZeroMQ messages** with clearly defined roles and framing. The major operations are:

- `get`: request the current value of a key
- `set`: assign a value to a key
- `config`: request metadata/config for a store
- `ack`: early confirmation that a request was received
- `response`: actual reply to `get` or `set`
- `error`: structured failure message
- publish: unsolicited updates from daemons published via their zmq publish socket.

Message types and framing are defined in Section 3.

Additional notes:

- A response is **optionally** preceded by an `ack`, if the reply is not within a small timeout the lack of an `ack` will indicate failure,.
- **Bulk binary payloads** are sent as an additional frame after the JSON payload and shall be described in the JSON
- **Bundles** of related values may be got/set under a common keypath superset, e.g., `guider.exposure` could include a nested json dict of `key`:`key_dict`
  - Implementation would presently be at the daemon / handler level vs the MKTLComs object.
  - Clients are not required to support bundled get or set requests (nb this is harder on embedded systems)  


### 2.5 Roles of Registry and Discovery
**OBSOLETE** zyre support for decentralized registration has been added. 

One or more KTL-mKTL bridge daemon(s) are required to act as a bridge between the KTL and mKTL land. 

~~The **Registry** daemon maintains knowledge of active keys and their owning `MKTLComs` identities and addresses. 
It is queried by other `MKTLComs` instances as needed, and optionally pushes updates (via `set`) to daemons that have previously queried it.~~

~~The Registry does not function as a central broker—it simply assists with initial discovery and consistency of routing.~~

- Each daemon registers its keys dynamically
- `MKTLComs` requests config and identity info as needed without higher level intervention
- **Static configuration** is used at a daemon level for legacy bridges to KTL

### 2.6 mKTLComs and Communication Patterns

The `MKTLComs` object is the primary communication interface for daemons and tools. It:

- Employs the zyre and zgossip RFCs of zmq to "whisper" between `MKTLComs` and perform autodiscovery of peers
  - **Makes it so that all the daemon needs to know is the mKTL key to get or set or what it wants to do with the argumets sent in a request a key it serves**
- Publishes via a `PUB` socket
- Subscribes via a `SUB` socket
- Provides client code with `MKTLMessage` objetes that:
  - make `ack()`, `respond()`, `fail()` trivial, and hides JSON (de)serialization
  - provides direct access to the raw JSON and binary data
- Offers thread-safe subscription queues and and callbacks

Lightweight `MKTLComs` instances can be transient, though rapidly crerating and destrying them is not recommended


## 3. Message Architecture

mKTL uses **ZeroMQ multipart messages** per the zyre RFC for all communication. This allows for clean separation of 
routing, message metadata, and payload content—including bulk binary data when needed. The **Publish/Subscribe** 
pattern is also used.

### 3.1 ZeroMQ Socket Roles

| Pattern          | Role                     | Description                                                               |
|------------------|--------------------------|---------------------------------------------------------------------------|
| zyre      | `WHISPER`                | Used for directed operations: get, set, config, ack, response, error |
| Publish/Subscribe | `PUB/SUB` or `XPUB/XSUB` | Used for unsolicited updates and broadcasts                               |


mKTL explicitly avoids using ZeroMQ’s `REQ/REP` or `DEALER/ROUTER` sockets directly instear allying zyre to handle this for better performance.
zyre, however is a relative thin layer so embedded clients are quite manageable.


### 3.2 Framing Model

All mKTL messages are **multipart** and follow consistent framing conventions.

### 3.3 Message Format

| Frame # | Name                | Description                                                       |
|---------|---------------------|-------------------------------------------------------------------|
| 0       | ZYRE_TYPE           | zyre rfc:  message type, mKTL messages are always `WHISPER`       |
| 1       | PEER_ID             | zyre rfc: Identifies the message origin                           |
| 2       | PEER_NAME           | zyre rfc: peers's name                                            |
| 3       | `MESSAGE_TYPE`      | e.g. `b"get"`, `b"set"`, `b"ack"`, `b"response"`, `b"error"`      |
| 4       | `REQUEST_ID`        | UUID bytes to match responses to requests.                        |
| 5       | `KEY`               | Dot-delimited keypath (e.g., `"adc.enabled"`).                    |
| 6       | `mKTL_JSON_PAYLOAD` | Required. Encodes parameters, return values, metadata, etc.       |
| 7       | `BULK_PAYLOAD`      | Optional. Present only if binary content is being transferred.    |

### 3.4 Supported Message Types

| Type       | Purpose                                               |
|------------|-------------------------------------------------------|
| `get`      | Query the current value of a key                      |
| `set`      | Assign a value to a key                               |
| `ack`      | Acknowledge receipt of a request (always precedes response unless immediate) |
| `response` | Fulfillment of a `get` or `set` operation             |
| `error`    | Failure or rejection (invalid key, bad format, etc.)  |
| `config`   | Request the configuration (StoreConfig) of a Store    |


### 3.5 Publish/Subscribe Message Format

| Frame # | Name           | Description                                                              |
|---------|----------------|--------------------------------------------------------------------------|
| 0       | `TOPIC`        | A keypath or `bulk:` prefixed keypath (e.g., `adc.enabled`, `bulk:camera.image`) |
| 1       | `JSON_PAYLOAD` | Always present. Carries value, timestamp, and metadata                   |
| 2       | `BULK_PAYLOAD` | Optional. Included only for binary data, and only for `bulk:` topics     |

### 3.6 Subscribing by Topic

Thanks to ZeroMQ’s prefix-based subscription model:

- `SUBSCRIBE "adc."` receives all non-bulk messages for the `adc` namespace
- `SUBSCRIBE "bulk:adc."` receives only bulk binary messages
- Subscribers **never receive bulk messages** unless explicitly opted in via `bulk:` topics



## 4. Configuration Format

Each mKTL **Store** is associated with a **StoreConfig**—a structured dictionary that defines the keys in that store, their metadata, and optional default values. This configuration can be loaded statically, constructed dynamically at runtime, or queried using a `config` request.

Configurations describe key **capabilities and expectations**, not their current values. Runtime state is communicated via `get`, `set`, or `publish` messages.

### 4.1 Structure of a StoreConfig

A StoreConfig is a dictionary with the following top-level keys:

- `name`: The store’s canonical name (e.g., `"adc"`)
- `keys`: A nested dictionary where each key is a dot-delimited `keypath` string

Each key entry contains a metadata dictionary.

### 4.2 Key Metadata Fields

| Field           | Description                                                                   |
|------------------|-------------------------------------------------------------------------------|
| `type`           | Declares the type of the key’s value. See below for supported types.          |
| `readonly`       | Boolean. If true, external `set` operations are not allowed.                  |
| `default`        | Optional. The default or startup value.                                       |
| `value`          | (Runtime only) The current value, returned in a CONFIG response.              |
| `units`          | Optional string describing physical units (e.g. `"nm"`, `"V"`, `"K"`).        |
| `doc`            | A short human-readable description of the key’s purpose.                      |
| `last_updated`   | (Runtime only) ISO 8601 timestamp of last update.                             |
| `hash`           | (Optional) Hash of the current value for caching or deduplication.            |
| `keypath`        | Optional. Canonical hierarchical path; useful if flat keys used as dict keys. |

Additional optional fields may include:
- `allowed_values` (for enums or constrained sets)
- `visibility` (e.g., `"user"`, `"expert"`, `"hidden"`)
- `update_rate_hint` (suggested frequency in Hz)
- `dynamic` (boolean; true if key may appear/disappear during runtime)

### 4.3 Supported Types

| Type          | Description                                 |
|---------------|---------------------------------------------|
| `int`         | Integer value                               |
| `float`       | Floating-point number                       |
| `str`         | UTF-8 string                                |
| `bool`        | Boolean                                     |
| `enum`        | Enumerated set of string values             |
| `json`        | Arbitrary structured JSON object            |
| `binary`      | Large or raw binary payloads (e.g., images) |
| `timestamp`   | ISO-formatted datetime                      |
| `duration`    | Elapsed time or interval (in seconds)       |
| `int_array`   | Array of integers                           |
| `float_array` | Array of floats                             |
| `status`      | Structured operational state object         |

Custom types may be defined locally and interpreted by application convention.

### 4.4 Configuration Discovery

A client may request the configuration for a store using a `config` message:

```json
{
  "type": "config",
  "store": "guider",
  "id": "abc123"
}
```

Response:

```json
{
  "type": "response",
  "store": "guider",
  "id": "abc123",
  "data": {
    "guider.enabled": {
      "type": "bool",
      "readonly": false,
      "default": true,
      "doc": "Enable or disable guider"
    }
  }
}
```

Configuration discovery is enabled by the **participating endpoints themselves**, which declare key definitions at runtime. There is **no global config file**. Each control daemon is responsible for its own declarations.

For compatibility with legacy systems (e.g. KTL), daemons may load static configuration from **YAML** or **JSON** files. This is essential for proxy daemons or passive monitors of legacy keys.




## 5. Extensibility & Compatibility

The mKTL protocol is designed to remain robust and flexible over the long operational lifetime of observatory instruments. Its architecture supports forward-compatible extensions and gracefully accommodates legacy systems and clients.

### 5.1 Encoding Flexibility

Although mKTL messages currently use **JSON** for all semantic content (in the `JSON_PAYLOAD` frame), the protocol explicitly allows for future extensibility:

- The transport framing is agnostic to encoding.
- A future version could support alternatives like **YAML**, **CBOR**, or **MessagePack** if:
  - Declared clearly in `MESSAGE_TYPE` or metadata
  - Interoperability is maintained via optional fallback
- The `MKTLComs` layer can abstract encoding details from application logic.

### 5.2 Binary Payloads and Framing

All binary data is transmitted as a **separate multipart frame**, always following the JSON frame, and only included when needed.

- Binary frames are never base64-encoded or embedded in JSON.
- Intermediate components (e.g., caches, routers) can discard bulk frames while preserving routing metadata and JSON semantics.
- Bulk messages use `bulk:`-prefixed topics to enable clean filtering.

This separation ensures high-throughput applications (e.g., camera daemons) don’t impact telemetry or control channels.

### 5.3 Identity and Routing

Each participant in the system should have an explicitly assigned **ZeroMQ identity** to support:

- Consistent routing of multipart messages
- Direct point-to-point communication (e.g., via `MKTLComs`)
- Tracing and logging via identity headers

This identity should be:
- Deterministically derived from the store's configuration and purpose
- Stable and human-meaningful for debugging
- Only randomly assigned by ZeroMQ in test or fallback conditions

Examples:
- `"guider.statusd"`
- `"adc.controller"`
- `"sensor_mux_1"`

### 5.4 Graceful Degradation

mKTL is built to operate in heterogeneous environments:

- **Legacy KTL proxies** can use static key declarations
- **Minimal clients** can omit support for bulk frames, config queries, or subscriptions
- **Downstream caches** may suppress or archive subsets of message traffic (e.g., discarding binary while retaining JSON)

This allows multiple daemons and tools to coexist and interact without imposing a single version or capability set across the full system.

### 5.5 Configuration Injection and Static Keys

In some cases—such as proxying legacy systems or serving passive telemetry—keys may be declared via static configuration files in **YAML** or **JSON**.

- These files may be loaded by a `Store` implementation on startup.
- Used primarily where the underlying system cannot participate in dynamic key advertisement.
- A format specification and examples are provided in the Appendix.

This mechanism enables tools like KTL-to-mKTL bridges to expose rich metadata and support config discovery, even when the underlying system is unaware of mKTL.



## 6. Compatibility and Migration

The modern mKTL protocol is both conceptually descended from and operationally distinct from the legacy KTL and early mKTL implementations. It intentionally maintains compatibility at the level of **key naming**, **value semantics**, and **message types**, while significantly improving transport, clarity, and modularity.

### 6.1 Key Naming Compatibility

- Legacy KTL uses a `service.key` naming model.
- mKTL adopts a general `store.keypath` convention, where `store` is the first segment of the key.
- Legacy flat keys are fully supported; hierarchical keypaths provide a clean upgrade path.

This ensures:
- New systems can adopt expressive key naming
- Legacy interfaces remain usable without renaming or rewriting

### 6.2 Message Type Alignment

| Legacy Type | New Type     | Notes                                                     |
|-------------|--------------|-----------------------------------------------------------|
| `get`       | `get`        | Retained                                                  |
| `set`       | `set`        | Retained                                                  |
| `ack`       | `ack`        | Required for delayed responses                            |
| `fail`      | `error`      | Renamed for clarity and common idiom                      |
| `config`    | `config`     | Retained; query for StoreConfig                           |
| `monitor`   | *(removed)*  | Now implied via SUB sockets; no request necessary         |

The type `ack` is explicitly required to precede a `response` unless the response is immediate. This provides a consistent signal path for daemons and clients to detect delivery failures.

### 6.3 Decentralized Configuration

Older systems assumed a single source of truth for configuration (e.g. `.cfg` or `.ini` files). mKTL shifts this model:

- Stores declare keys dynamically during runtime.
- Static configuration files (in YAML/JSON) may supplement when proxies or legacy systems are used.
- A centralized **Registry** may aggregate and serve configuration responses for discovery, but is not mandatory.

### 6.4 Identity and Deployment Consistency

Each `MKTLComs` instance should be instantiated with a clearly identifiable **ZeroMQ identity** derived from programmatic configuration. This identity reflects the daemon or process's role in the system.

Examples:
- `"guider.statusd"`
- `"adc.controller"`
- `"sensor_mux_1"`

If no identity is provided, one may be assigned by ZeroMQ—but this is considered a development-only fallback. Logging, debugging, and routing diagnostics rely on stable identity assignment.

### 6.5 Phased Migration and Coexistence

mKTL can coexist with existing KTL or early mKTL systems through:

- Proxy daemons that expose legacy services via mKTL APIs
- Config-injection bridges for static keys
- Clients that support both KTL and mKTL at runtime

This enables observatory systems to adopt the new architecture **incrementally**, without disruptive rewrites.



## 7. Appendices

**OUTDATED: Describes what was used for LRIS2 CSU demo review, not current with zyre enhancements.**


### 7.1 Framed Request/Response Message Examples

These examples show **actual ZeroMQ multipart message frames** using Python-style notation. Each element in the list is a `bytes` object.


#### ➤ `get` Request and Response

**Sent by DEALER (e.g., GUI client):**
```python
[
    b'get',
    b'req-001',
    b'adc.enabled',
    b'{}'
]
```

**Received at ROUTER (daemon):**
```python
[
    b'client-01',
    b'',
    b'get',
    b'req-001',
    b'adc.enabled',
    b'{}'
]
```

**ACK:**
```python
[
    b'client-01',
    b'',
    b'ack',
    b'req-001',
    b'adc.enabled',
    b'{"pending": true}'
]
```

**Response:**
```python
[
    b'client-01',
    b'',
    b'response',
    b'req-001',
    b'adc.enabled',
    b'{"value": true, "timestamp": "2025-04-05T18:30:00Z"}'
]
```

#### ➤ `set` and Error

**Set Request:**
```python
[
    b'set',
    b'req-002',
    b'adc.temperature.board',
    b'{"value": 12.5}'
]
```

**Error Response (readonly key):**
```python
[
    b'client-01',
    b'',
    b'error',
    b'req-002',
    b'adc.temperature.board',
    b'{"error": "readonly key"}'
]
```

---

### 7.2 Framed Publish Examples

#### ➤ JSON-only
```python
[
    b'adc.temperature.board',
    b'{"value": 22.4, "timestamp": "2025-04-05T18:34:20Z"}'
]
```

#### ➤ JSON + Binary
```python
[
    b'bulk:camera.frame',
    b'{"timestamp": "2025-04-05T18:34:50Z", "exposure_id": 87423}',
    <binary_image_data>
]
```

---

### 7.3 Subscription Filter Summary

| Subscription Filter   | Matches                               | Skips                               |
|------------------------|----------------------------------------|--------------------------------------|
| `b"adc."`             | All JSON updates to keys in `adc.`     | All `bulk:` prefixed updates         |
| `b"bulk:camera."`     | Only binary messages for `camera.`     | All normal telemetry for `camera.`   |

---

### 7.4 Static Configuration Format (YAML)

Potential example of YAML config

```yaml
name: camera
keys:
  camera.exposure_time:
    type: float
    units: s
    default: 1.0
    doc: Exposure time in seconds
    readonly: false

  camera.gain:
    type: enum
    allowed_values: [low, medium, high]
    default: medium
    doc: Amplifier gain setting
    readonly: false

  camera.frame:
    type: binary
    doc: Latest image frame
    readonly: true
```

---

### 7.5 Reserved and Recommended Types

| Type          | Notes                                                  |
|---------------|--------------------------------------------------------|
| `int`         | Integer                                                |
| `float`       | Floating point                                         |
| `str`         | UTF-8 string                                           |
| `bool`        | Boolean                                                |
| `enum`        | Enumerated allowed values (strings)                    |
| `json`        | Structured object                                      |
| `binary`      | Large non-JSON data                                    |
| `timestamp`   | ISO 8601 format (`"2025-04-05T13:30:00Z"`)             |
| `duration`    | Float, seconds                                          |
| `status`      | Named structure used for summary or heartbeat fields   |


