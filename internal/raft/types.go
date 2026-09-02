package raft

type NodeID string
type Term uint64
type LogIndex uint64

type RaftRole uint8

const (
	RaftFollower RaftRole = iota
	RaftCandidate
	RaftLeader
)

type RoleTransition uint8

const (
	BecomeFollower RoleTransition = iota
	BecomeCandidate
	BecomeLeader
	KeepCurrentRole
)

// HardState contains the state of the Raft node that needs to survive crashes - CurrentTerm and VotedFor
type HardState struct {
	CurrentTerm Term
	// empty string represents no vote
	VotedFor NodeID
}
