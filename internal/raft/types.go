package raft

type NodeID string
type Term uint64

type HardState struct {
	CurrentTerm Term
	// empty string represents no vote
	VotedFor NodeID
}
