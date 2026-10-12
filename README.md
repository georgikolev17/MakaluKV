# MakaluKV

**MakaluKV** is a distributed key-value store written in Go, built around a custom implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf).

The project focuses on replication, fault tolerance, durable state, and the internal mechanics of building a distributed system from the ground up.

## Why MakaluKV?

MakaluKV is a learning-focused systems project created to gain hands-on experience with:

- distributed consensus
- replication and fault tolerance
- durable storage
- RPC-based communication
- Go systems programming
- designing clear boundaries between consensus, storage, and application logic

Rather than using an existing Raft library, the consensus layer is being implemented from scratch.

## Features

### Implemented

- Persistent key-value storage
- gRPC API
- `Get`, `Put`, `Delete`, and Compare-and-Swap operations
- Pebble-backed persistence
- Persistent Raft hard state
  - current term
  - voted-for node
- Raft leader election
- Durable Raft log storage primitives

### In Progress

- Raft log replication
- `AppendEntries` consistency and conflict handling
- Leader replication state
- Commit index advancement

### Planned

- Applying committed log entries to the state machine
- Raft runtime with timers and RPC delivery
- Real 3-node clusters
- Failure recovery
- Linearizable reads
- Snapshots and log compaction
- Deterministic fault testing

## Architecture

MakaluKV separates the consensus layer from the replicated application state.

```text
                Client
                  |
                  v
               gRPC API
                  |
                  v
             Raft Runtime
                  |
                  v
              Raft Core
             /         \
            v           v
     Stable Storage   State Machine
       (Pebble)         (Pebble)
```

The two persistent stores serve different purposes:

```text
data/node-1/
├── raft/            # Raft term, vote, and replicated log
└── state-machine/   # Replicated key-value state
```

The Raft core contains protocol logic and coordinates persistence of Raft safety-critical state through a storage abstraction.

For more detail, see [DESIGN.md](docs/DESIGN.md).
