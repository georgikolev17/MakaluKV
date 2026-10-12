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

// HardState contains the state of the Raft node that needs to survive crashes - CurrentTerm and VotedFor
type HardState struct {
	CurrentTerm Term
	// empty string represents no vote
	VotedFor NodeID
}

type LogEntry struct {
	Term  Term
	Index LogIndex
	Data  []byte
}
