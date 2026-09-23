# MakaluKV Development Plan

## Main Goal

Build MakaluKV into a correct, persistent, fault-tolerant distributed key-value store backed by Raft.

The primary milestone is a real 3-node cluster that can:

- elect a leader;
- replicate client mutations;
- commit entries through Raft consensus;
- apply committed entries to the key-value state machine;
- persist Raft state across restarts;
- tolerate basic node failures and leader reelections.

The initial goal is correctness and understanding rather than production-level performance.

---

## Progress

| Stage | Status | Target |
|---|---|---|
| 1. Single-node key-value store | Completed | Done |
| 2. Raft persistent state | Completed | 30 Aug 2026 |
| 3. Leader election | In progress | 6 Sep 2026 |
| 4. Log replication | Not started | 20 Sep 2026 |
| 5. Apply committed entries | Not started | 27 Sep 2026 |
| 6. Raft runtime | Not started | 11 Oct 2026 |
| 7. Real 3-node cluster | Not started | 18 Oct 2026 |
| 8. Raft visualization GUI | Not started | 1 Nov 2026 |

---

# Stage 1 — Single-Node Key-Value Store

**Status:** Completed

Implement the initial standalone MakaluKV server.

### Work

- [x] Define client-facing Protobuf API.
- [x] Implement `Get`.
- [x] Implement `Put`.
- [x] Implement `Delete`.
- [x] Implement `CompareAndSwap`.
- [x] Implement Pebble-backed state-machine storage.
- [x] Add service layer.
- [x] Add gRPC server.
- [x] Add request validation.
- [x] Add gRPC error translation.
- [x] Add graceful shutdown.
- [x] Add storage tests.
- [x] Add gRPC integration test.

### Result

A single MakaluKV process operates as a durable key-value server.

---

# Stage 2 — Raft Persistent State

**Status:** Completed  
**Target:** 30 August 2026

Introduce the durable state required by the Raft protocol.

### Persistent state

For now:

- `CurrentTerm`
- `VotedFor`

Later:

- Raft log

### Work

- [x] Define Raft domain types such as `NodeID`, `Term`, and `HardState`.
- [x] Define the `StableStorage` abstraction.
- [x] Separate Raft persistence from state-machine persistence.
- [x] Create Pebble-backed Raft storage.
- [x] Persist `CurrentTerm` and `VotedFor` atomically.
- [x] Initialize fresh Raft storage safely.
- [x] Detect malformed or inconsistent persisted hard state.
- [x] Verify hard state survives closing and reopening the database.
- [x] Add unit tests.

### Result

A restarted Raft node can recover the persistent state required to safely participate in future elections.

---

# Stage 3 — Leader Election

**Status:** In progress  
**Target:** 21 September 2026

Implement leader election as part of the deterministic Raft core.

The Raft core should not depend on:

- gRPC;
- real timers;
- goroutines;
- channels;
- Pebble;
- networking.

### Work

- [x] Initialize a node from persisted `HardState`.
- [x] Define follower, candidate, and leader roles.
- [ ] TODO: After Stage 6 -> Handle election timeout.
- [x] Implement follower → candidate transition.
- [x] Increment the term when starting an election.
- [x] Vote for self.
- [x] Generate `RequestVote` messages.
- [x] Handle incoming `RequestVote`.
- [x] Handle `RequestVoteResponse`.
- [x] Count votes.
- [x] Detect majority.
- [x] Implement candidate → leader transition.
- [x] Handle higher terms received in Raft messages.
- [x] Step down to follower when required.
- [x] Handle empty `AppendEntries` heartbeats.
- [x] Add deterministic leader-election tests.
- [ ] Document the implemented changes

### Important scenarios to test

- [ ] TODO for later: One node times out and wins an election.
- [x] Candidate receives enough votes to become leader.
- [ ] Candidate fails to receive a majority.
- [ ] Two candidates compete in the same term.
- [ ] Node refuses to vote twice in the same term.
- [x] Candidate receives a higher-term message and becomes follower.
- [x] Leader receives a higher-term message and becomes follower.
- [ ] Valid heartbeat prevents unnecessary election.

### Result

Multiple deterministic Raft cores can elect a leader correctly without using real networking or concurrency.

---

# Stage 4 — Log Replication

**Status:** Not started  
**Target:** 4 October 2026

Extend the Raft core with replicated logs.

### Work

- [ ] Define `LogEntry`.
- [ ] Extend `StableStorage` with log persistence operations.
- [ ] Persist log entries in the Raft Pebble store.
- [ ] Extend `AppendEntries` with log entries.
- [ ] Implement `PrevLogIndex` and `PrevLogTerm` validation.
- [ ] Implement follower log consistency checks.
- [ ] Implement conflicting-entry removal.
- [ ] Implement log append behavior.
- [ ] Add leader `nextIndex` state.
- [ ] Add leader `matchIndex` state.
- [ ] Implement replication to followers.
- [ ] Handle `AppendEntriesResponse`.
- [ ] Retry replication for followers that are behind.
- [ ] Implement commit-index advancement.
- [ ] Enforce Raft's commit rules.

### Important scenarios to test

- [ ] Normal log replication.
- [ ] Follower missing entries.
- [ ] Follower containing conflicting entries.
- [ ] Leader retries using earlier log indices.
- [ ] Majority replication commits an entry.
- [ ] Minority replication does not commit an entry.
- [ ] New leader repairs inconsistent follower logs.

### Result

A leader can replicate commands to followers and determine when log entries are committed.

---

# Stage 5 — Apply Committed Entries

**Status:** Not started  
**Target:** 27 September 2026

Connect committed Raft entries to the MakaluKV state machine.

### Work

- [ ] Define the replicated command format.
- [ ] Represent `Put` as a Raft command.
- [ ] Represent `Delete` as a Raft command.
- [ ] Represent `CompareAndSwap` as a Raft command.
- [ ] Apply committed entries strictly in log order.
- [ ] Track `lastApplied`.
- [ ] Persist the state machine's applied index.
- [ ] Update the applied index atomically with state-machine mutations.
- [ ] Prevent already-applied commands from being applied again after restart.
- [ ] Return command results after commit and application.
- [ ] Verify CAS produces deterministic results.

### Result

The replicated Raft log becomes the authoritative source of mutations to the MakaluKV state machine.

---

# Stage 6 — Raft Runtime

**Status:** Not started  
**Target:** 11 October 2026

Build the concurrent runtime around the deterministic Raft core.

### Architecture

```text
Timers / RPCs / Clients
         |
         v
     Raft Runtime
         |
         v
      Raft Core
         |
         +---- Stable Storage
         |
         +---- State Machine
```

### Work

- [ ] Create a single-owner Raft event loop.
- [ ] Add election timer.
- [ ] Add randomized election timeouts.
- [ ] Add leader heartbeat scheduling.
- [ ] Deliver incoming Raft messages to the event loop.
- [ ] Ensure only the event loop mutates Raft core state.
- [ ] Persist required state before dependent messages are sent.
- [ ] Add outbound peer transport workers.
- [ ] Ensure slow peers cannot block the Raft event loop.
- [ ] Add ordered state-machine applier.
- [ ] Add client proposal handling.
- [ ] Track pending client proposals.
- [ ] Notify clients when commands commit and apply.
- [ ] Handle shutdown cleanly.

### Result

The deterministic Raft implementation operates correctly inside a real concurrent server process.

---

# Stage 7 — Real 3-Node Cluster

**Status:** Not started  
**Target:** 18 October 2026

Run MakaluKV as a real distributed system.

This is the primary milestone of the current project plan.

### Work

- [ ] Define internal Raft Protobuf RPCs.
- [ ] Implement `RequestVote` RPC.
- [ ] Implement `AppendEntries` RPC.
- [ ] Build the Raft gRPC adapter.
- [ ] Build peer-to-peer Raft transport.
- [ ] Configure stable node IDs.
- [ ] Configure peer addresses.
- [ ] Give each node its own Raft data directory.
- [ ] Give each node its own state-machine data directory.
- [ ] Run three MakaluKV processes locally.
- [ ] Route mutations through Raft.
- [ ] Handle requests sent to followers.
- [ ] Expose leader information where useful.

### Basic failure testing

- [ ] Kill the current leader.
- [ ] Verify a new leader is elected.
- [ ] Restart the old leader.
- [ ] Verify it rejoins as a follower.
- [ ] Stop one follower.
- [ ] Verify the remaining majority continues operating.
- [ ] Restart a follower and verify it catches up.
- [ ] Simulate a temporary network partition.
- [ ] Verify the majority side can elect or retain a leader.
- [ ] Verify logs converge after connectivity is restored.

### Result

MakaluKV operates as a persistent, fault-tolerant 3-node replicated key-value cluster.

---

# Stage 8 — Raft Visualization GUI

**Status:** Not started  
**Target:** 1 November 2026

Build a GUI focused on visualizing the distributed system rather than simply providing a CRUD interface.

---

# Current Focus

**Stage 3 — Leader Election**