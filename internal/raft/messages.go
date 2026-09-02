package raft

type RequestVote struct {
	Term         Term
	CandidateID  NodeID
	LastLogIndex LogIndex
	LastLogTerm  Term
}

type RequestVoteResponse struct {
	ResponderID NodeID
	Term        Term
	VoteGranted bool
}

type AppendEntries struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex LogIndex
	PrevLogTerm  LogIndex
	// TODO: Add Entries field to AppendEntries struct
	// Entries      []LogEntry
	LeaderCommit LogIndex
}

type AppendEntriesResponse struct {
	Term    uint64
	Success bool
}
