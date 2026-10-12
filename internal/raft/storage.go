package raft

// The Store interface is necessary for each node so that it can store its persistent Raft state. The persistent Raft state consists of:
// 1. CurrentTerm - the current term of the Raft algorithm
// 2. VotedFor - the id of the node for which the current node has voted in this term
// 3. Logs - list of all received logs
// We call VotedFor and CurrentTerm "hard state"

type StableStorage interface {
	// Returns the persisted Raft term and voted-for node id
	LoadHardState() (HardState, error)
	// Persists the new Raft term and voted-for node id
	SaveHardState(HardState) error

	// TODO: Logs implementation
	// AddLog() error
	// GetAllLogs() []LogEntry, error
	// GetLog(id uint64) LogEntry

	LastLogInfo() (LogIndex, Term, error)
	LogEntry(index LogIndex) (LogEntry, bool, error)
	LogEntries(from LogIndex, to LogIndex) ([]LogEntry, error)
	ReplaceLogSuffix(from LogIndex, logs []LogEntry) error
}
