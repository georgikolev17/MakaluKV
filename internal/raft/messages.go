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
	Term         Term
	LeaderID     NodeID
	PrevLogIndex LogIndex
	PrevLogTerm  Term
	// TODO: Add Entries field to AppendEntries struct
	Entries      []LogEntry
	LeaderCommit LogIndex // leader's commit index
}

type AppendEntriesResponse struct {
	Term    Term
	Success bool
}
