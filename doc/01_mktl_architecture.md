# `00_mktl_architecture.md`

## 🛰️ mKTL System Architecture and Operational Design

---

## 1. Overview

The **modern Keck Task Library (mKTL)** is a decentralized, ZeroMQ-based messaging framework designed for controlling and coordinating astronomical instrumentation at the W. M. Keck Observatory.

Unlike legacy KTL and centralized broker-based systems, mKTL emphasizes:
- **Distributed ownership of keys**
- **Direct peer-to-peer messaging**
- **Minimal centralized infrastructure**
- **Clean extensibility for new instrument classes**

This document outlines the **architecture, operational intent, and system design** of `MKTLComs`, the `Registry`, and the surrounding ecosystem of software entities.

---

## 2. Core Components

### 2.1 `MKTLComs` — Communication and Routing Engine

Each software participant (e.g. control daemon, monitoring GUI, sequencer) instantiates an `MKTLComs` object that handles:

- Sending and receiving **requests and replies** (`get`, `set`, `ack`, `response`, `error`)
- Managing **ZeroMQ sockets** (`ROUTER`, `DEALER`, `PUB`, `SUB`)
- Publishing **telemetry data** with optional binary payloads
- Subscribing to updates and receiving them via callbacks or iterable listeners
- Responding to requests for **authoritative keys** through registered handlers

Key design traits:

- Identity is **settable** and **used for all routing**
- Network interaction is fully handled in **background threads**
- Supports both **blocking and non-blocking** execution paths (but does not use `asyncio`)
- Clean separation of transport logic and user application code
- Optional registry discovery and caching

> `MKTLComs` is the sole interface layer between application logic and the mKTL transport. Codebases using it never deal directly with ZMQ.

---

### 2.2 Registry — Lightweight Discovery and Coordination

The `Registry` daemon runs a separate `MKTLComs` instance and serves three primary roles:

1. **Resolve key ownership**: Who owns `adc.temperature`?
2. **Push updated routing info** to connected clients when keys change
3. **Store configuration snapshots** for passive inspection or bridging

Keys are registered **dynamically** at runtime by each daemon. There is no global config file.

Configuration and registration are handled using **normal `get` and `set` messages**, not a dedicated `config` message type. This unifies protocol behavior and simplifies parsing. For example, a `set` to `registry.config` or a `get` from `registry.owner` performs introspection or state registration. mKTL daemons may also expose `.mktl_control` keys to respond to registry coordination messages.

The registry is **optional** at runtime, but is central to:
- First-time contact between systems
- Ensuring scalable routing
- Supporting proxies to legacy systems

> Note: Routing updates are issued as direct `set` messages to connected peers, not as PUB/SUB broadcasts.
---

## 3. Architectural Model

mKTL daemons follow a **flat peer model**:

- Every daemon **binds** a `ROUTER` socket for inbound control
- Every daemon **connects** a `DEALER` socket to peers for outbound commands
- Most daemons **bind a PUB** socket and optionally **connect to others’ PUBs** via `SUB`

No centralized “router” or “hub” exists — unless explicitly designed via proxies or bridges.

---

## 4. Lifecycle of a Key

| Phase         | Action |
|---------------|--------|
| Declaration   | On startup, a daemon declares the keys it serves |
| Registration  | Daemon informs Registry of its keys and address |
| Resolution    | Another peer asks the Registry: “who owns `adc.temperature`?” |
| Communication | Peer sends request directly to owner |
| **Response**  | Owner replies to requester with result or error |
| Publishing    | Owner publishes updates on `adc.temperature` to all subscribers |

---

## 5. Peer Roles in mKTL

### 5.1 Control Daemons
- Control physical devices or subsystems (e.g., filter wheels, cameras)
- Own one or more keys
- Respond to `get` and `set` operations
- May publish telemetry updates
- Examples:
  - `adc.controller`
  - `filterwheel.motor`
  - `guider.statusd`

### 5.2 Observing GUIs and Tools
- Issue `get`/`set` requests
- Subscribe to telemetry
- May offer scripting endpoints (and thus declare command keys)
- Typically **connect only**, not bind
- May respond to commands from other tools

### 5.3 Coordinator Daemons
- Coordinate multi-subsystem sequences (e.g., alignments, calibrations)
- Subscribe to many keys, issue commands to multiple endpoints
- May serve **composite keys** (e.g., `guider.sequence.status`)
- Internally use MKTLComs like all other peers

### 5.4 Proxies and Bridges
- Mediate between mKTL and legacy systems (e.g., classic KTL)
- May declare keys that are static or loaded from config files
- Typically bind both PUB and ROUTER, and connect DEALER and SUB
- Examples:
  - `ktl.bridge.imager`
  - `legacy.status.proxyd`

---

## 6. Error Handling and Robustness

- Any **malformed message** triggers an immediate `error` response
- All exceptions from handlers are caught and returned as `error`
- `ack` is required for requests unless the response is truly immediate
- Bulk data can be stripped by intermediate systems (e.g., caches)
- Fault tolerance assumes peer-by-peer isolation — no single point of failure

---

## 7. Registry Integration Details

Each `MKTLComs` instance can be configured with:

```python
coms = MKTLComs(
    identity="guider.statusd",
    registry_addr="tcp://registry:5555",
    authoritative_keys={
        "guider.status": handle_status,
        "guider.enable": handle_enable,
        "guider.statusd.mktl_control": handle_control
    }
)
```
It will:
- Connect to the Registry on startup
- Register its keys and address
- Optionally listen for route updates or config snapshots
- Receive registry messages via its internal message queue

---

## 8. Extensibility and Future-Proofing

- Key support for binary + JSON separation
- Protocol supports multiple encodings
- Clear type schema and default metadata for all keys
- Configs can be loaded from YAML or dynamically discovered
- Bundle messages (e.g., atomic group `set`s) built on top of existing primitives

---

## 9. Integration and Usage Philosophy

mKTL is designed to allow scientists, engineers, and systems integrators to build observatory control logic **without requiring deep knowledge of ZeroMQ**.

It embraces:
- **Tight coupling to existing Python systems**
- **Minimal boilerplate for simple tasks**
- **Explicit routing and naming** to aid debugging
- **Symmetric message flow** so that any tool can both serve and request