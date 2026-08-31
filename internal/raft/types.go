package raft

type NodeID string
type Term uint64

// HardState contains the state of the Raft node that needs to survive crashes - CurrentTerm and VotedFor
type HardState struct {
	CurrentTerm Term
	// empty string represents no vote
	VotedFor NodeID
}
