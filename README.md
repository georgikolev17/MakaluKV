# MakaluKV Functionality

MakaluKV is a distributed key-value store that provides persistent, replicated, and strongly consistent storage across multiple nodes.

## Core Operations

### Get

Retrieves the value associated with a key.

* Returns the current value when the key exists.
* Returns a clear `not found` result when the key does not exist.
* Strongly consistent reads must not return an older value.

### Put

Creates a new key or replaces the value of an existing key.

* Creates the key when it does not exist.
* Replaces the current value when the key already exists.
* Reports success only after the operation satisfies the required replication and durability guarantees.

### Delete

Removes a key and its associated value.

* Removes the key when it exists.
* Deleting a missing key is treated as successful and leaves the database unchanged.
* Repeating the same deletion does not create additional effects.

### Compare-and-Swap

Updates a key only when its current value matches an expected value.

* Accepts a key, an expected value, and a replacement value.
* Performs the comparison and update as one atomic operation.
* Returns whether the comparison succeeded.
* Leaves the value unchanged when the comparison fails.

## Persistent Storage

MakaluKV stores data on disk so that acknowledged operations survive process and node restarts.

* A restarted node recovers its previous state.
* Successfully acknowledged mutations must not be lost after a leader failure.
* Incomplete recovery or snapshot data must never become visible to clients.

## Client Communication

Clients communicate with MakaluKV through a gRPC API.

* Clients can send requests to any known node.
* Nodes that are not the leader can return information about the current leader.
* Clients can retry requests after temporary failures.
* Requests support deadlines and cancellation.

## Replication

MakaluKV replicates operations across three nodes.

* One node acts as the leader.
* The remaining nodes act as followers.
* Mutations are ordered and replicated through Raft.
* A mutation is committed only after it is accepted by a majority of replicas.
* The cluster remains operational when one node is unavailable.
* A single isolated node cannot commit new mutations.
* Restarted or delayed followers eventually catch up with the leader.

## Leader-Based Operations

The current leader coordinates mutations and strongly consistent reads.

* Writes are handled by the leader.
* Strongly consistent reads are handled through the leader.
* Followers must not return stale data as though it were strongly consistent.
* Losing the leader may cause temporary unavailability while a new leader is elected.

## Linearizable Consistency

MakaluKV provides linearizable behaviour for its core operations.

* Completed operations appear to happen one at a time.
* Their apparent order respects real-time completion.
* After a write succeeds, later successful strong reads observe that write or a newer value.
* Conflicting compare-and-swap operations cannot both succeed against the same previous value.

## Client Retries

Clients may retry requests when responses are lost or temporary errors occur.

* Each client has a stable identifier.
* Each mutation has a monotonically increasing request number.
* Retrying the same logical mutation must not apply it more than once.
* A timeout does not necessarily mean that the operation failed.
* Temporary and permanent errors must be distinguishable.

## Snapshot Recovery

MakaluKV uses snapshots to prevent replicated logs from growing indefinitely and to recover replicas that fall far behind.

* A snapshot represents a consistent database state.
* Old replicated log entries can be removed after they are included in a snapshot.
* A delayed replica can install a snapshot and then continue receiving newer operations.
* An incomplete snapshot installation must not replace the current valid state.

## Static Sharding

The final core version of MakaluKV divides the key space into fixed shards.

* Every key belongs to exactly one shard.
* Each shard has its own Raft group.
* Different shards may have different leaders.
* Requests are routed to the shard responsible for their key.
* Failure of one shard does not prevent healthy shards from operating.
* Atomic operations initially apply only within one shard.
* Cross-shard transactions are not part of the core version.
