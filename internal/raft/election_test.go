package raft

import (
	"errors"
	"testing"
)

// --- StartElection ---

func TestStartElectionPersistenceFailure(t *testing.T) {
	config := Config{
		NodeID:  "node-1",
		Members: []NodeID{"node-1", "node-2", "node-3"},
	}

	storage := &testStorage{
		hardState: HardState{
			CurrentTerm: 2,
			VotedFor:    "node-2",
		},
		saveErr: errors.New("save failed"),
	}

	core, err := NewRaftCore(config, storage)
	if err != nil {
		t.Fatal(err)
	}

	_, err = core.StartElection()
	if err == nil {
		t.Fatal("expected persistence error")
	}

	if core.currentTerm != 2 {
		t.Errorf("currentTerm = %d, want 2", core.currentTerm)
	}

	if core.role != RaftFollower {
		t.Errorf("role = %v, want %v", core.role, RaftFollower)
	}
}

func TestStartElectionSingleNodeBecomesLeader(t *testing.T) {
	config := Config{
		NodeID:  "node-1",
		Members: []NodeID{"node-1"},
	}

	core, err := NewRaftCore(config, &testStorage{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = core.StartElection()
	if err != nil {
		t.Fatal(err)
	}

	if core.role != RaftLeader {
		t.Errorf("role = %v, want %v", core.role, RaftLeader)
	}
}

func TestStartElectionBecomesCandidate(t *testing.T) {
	config := Config{
		NodeID:  "node-1",
		Members: []NodeID{"node-1", "node-2", "node-3"},
	}

	core, err := NewRaftCore(config, &testStorage{})
	if err != nil {
		t.Fatal(err)
	}

	req, err := core.StartElection()
	if err != nil {
		t.Fatal(err)
	}

	if core.role != RaftCandidate {
		t.Errorf("role = %v, want %v", core.role, RaftCandidate)
	}

	if core.currentTerm != 1 {
		t.Errorf("currentTerm = %d, want 1", core.currentTerm)
	}

	if core.votedFor != "node-1" {
		t.Errorf("votedFor = %q, want %q", core.votedFor, "node-1")
	}

	if req.CandidateID != "node-1" || req.Term != 1 {
		t.Errorf("unexpected RequestVote: %#v", req)
	}
}

// --- HandleVoteRequest ---

func TestHandleVoteRequestRejectsOlderTerm(t *testing.T) {
	config := Config{
		NodeID:  "node-1",
		Members: []NodeID{"node-1", "node-2", "node-3"},
	}
	storage := &testStorage{
		hardState: HardState{
			CurrentTerm: 2,
		},
	}

	core, err := NewRaftCore(config, storage)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := core.HandleVoteRequest(RequestVote{
		Term:        1,
		CandidateID: "node-2",
	})
	if err != nil {
		t.Fatal(err)
	}

	if resp.VoteGranted {
		t.Error("vote was granted to candidate from older term")
	}

	if core.currentTerm != 2 {
		t.Errorf("currentTerm = %d, want 2", core.currentTerm)
	}
}

func TestHandleVoteRequestHigherTermGrantsVoteAndBecomesFollower(t *testing.T) {
	config := Config{
		NodeID:  "node-1",
		Members: []NodeID{"node-1", "node-2", "node-3"},
	}
	storage := &testStorage{
		hardState: HardState{
			CurrentTerm: 2,
			VotedFor:    "node-1",
		},
	}

	core, err := NewRaftCore(config, storage)
	if err != nil {
		t.Fatal(err)
	}

	// To verify that the role is changed to Follower and not just stays as it is
	core.role = RaftCandidate

	resp, err := core.HandleVoteRequest(RequestVote{
		Term:        3,
		CandidateID: "node-2",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !resp.VoteGranted {
		t.Error("vote was not granted to candidate from higher term")
	}

	if core.role != RaftFollower {
		t.Errorf("role = %v, want %v", core.role, RaftFollower)
	}

	if core.currentTerm != 3 {
		t.Errorf("currentTerm = %d, want 3", core.currentTerm)
	}

	if core.votedFor != "node-2" {
		t.Errorf("votedFor = %q, want %q", core.votedFor, "node-2")
	}
}

func TestHandleVoteRequestCurrentTermRejectsDifferentCandidate(t *testing.T) {
	config := Config{
		NodeID:  "node-1",
		Members: []NodeID{"node-1", "node-2", "node-3"},
	}
	storage := &testStorage{
		hardState: HardState{
			CurrentTerm: 2,
			VotedFor:    "node-3",
		},
	}

	core, err := NewRaftCore(config, storage)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := core.HandleVoteRequest(RequestVote{
		Term:        2,
		CandidateID: "node-2",
	})
	if err != nil {
		t.Fatal(err)
	}

	if resp.VoteGranted {
		t.Error("vote was granted after already voting for another candidate")
	}

	if core.votedFor != "node-3" {
		t.Errorf("votedFor = %q, want %q", core.votedFor, "node-3")
	}
}

func TestHandleVoteRequestCurrentTermGrantsVoteWhenNotVoted(t *testing.T) {
	config := Config{
		NodeID:  "node-1",
		Members: []NodeID{"node-1", "node-2", "node-3"},
	}
	storage := &testStorage{
		hardState: HardState{
			CurrentTerm: 2,
		},
	}

	core, err := NewRaftCore(config, storage)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := core.HandleVoteRequest(RequestVote{
		Term:        2,
		CandidateID: "node-2",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !resp.VoteGranted {
		t.Error("vote was not granted despite node not voting in current term")
	}

	if core.votedFor != "node-2" {
		t.Errorf("votedFor = %q, want %q", core.votedFor, "node-2")
	}
}

func TestHandleVoteRequestCurrentTermGrantsRepeatedVoteToSameCandidate(t *testing.T) {
	config := Config{
		NodeID:  "node-1",
		Members: []NodeID{"node-1", "node-2", "node-3"},
	}
	storage := &testStorage{
		hardState: HardState{
			CurrentTerm: 2,
			VotedFor:    "node-2",
		},
	}

	core, err := NewRaftCore(config, storage)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := core.HandleVoteRequest(RequestVote{
		Term:        2,
		CandidateID: "node-2",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !resp.VoteGranted {
		t.Error("repeated vote request from same candidate was rejected")
	}
}

func TestHandleVoteRequestPersistenceFailure(t *testing.T) {
	config := Config{
		NodeID:  "node-1",
		Members: []NodeID{"node-1", "node-2", "node-3"},
	}
	storage := &testStorage{
		hardState: HardState{
			CurrentTerm: 2,
			VotedFor:    "node-1",
		},
		saveErr: errors.New("save failed"),
	}

	core, err := NewRaftCore(config, storage)
	if err != nil {
		t.Fatal(err)
	}

	core.role = RaftCandidate

	_, err = core.HandleVoteRequest(RequestVote{
		Term:        3,
		CandidateID: "node-2",
	})

	if err == nil {
		t.Fatal("expected persistence error")
	}

	if core.currentTerm != 2 {
		t.Errorf("currentTerm = %d, want 2", core.currentTerm)
	}

	if core.role != RaftCandidate {
		t.Errorf("role = %v, want %v", core.role, RaftCandidate)
	}

	if core.votedFor != "node-1" {
		t.Errorf("votedFor = %q, want %q", core.votedFor, "node-1")
	}
}

// --- HandleRequestVoteResponse ---

func TestHandleRequestVoteResponseHigherTermBecomesFollower(t *testing.T) {
	config := Config{
		NodeID:  "node-1",
		Members: []NodeID{"node-1", "node-2", "node-3"},
	}
	storage := &testStorage{
		hardState: HardState{
			CurrentTerm: 2,
			VotedFor:    "node-1",
		},
	}

	core, err := NewRaftCore(config, storage)
	if err != nil {
		t.Fatal(err)
	}

	core.role = RaftCandidate
	core.votesReceived[core.nodeID] = struct{}{}

	err = core.HandleRequestVoteResponse(RequestVoteResponse{
		ResponderID: "node-2",
		Term:        3,
		VoteGranted: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	if core.role != RaftFollower {
		t.Errorf("role = %v, want %v", core.role, RaftFollower)
	}

	if core.currentTerm != 3 {
		t.Errorf("currentTerm = %d, want 3", core.currentTerm)
	}

	if len(core.votesReceived) != 0 {
		t.Errorf("votesReceived length = %d, want 0", len(core.votesReceived))
	}
}

func TestHandleRequestVoteResponseMajorityBecomesLeader(t *testing.T) {
	config := Config{
		NodeID:  "node-1",
		Members: []NodeID{"node-1", "node-2", "node-3"},
	}
	storage := &testStorage{
		hardState: HardState{
			CurrentTerm: 2,
			VotedFor:    "node-1",
		},
	}

	core, err := NewRaftCore(config, storage)
	if err != nil {
		t.Fatal(err)
	}

	core.role = RaftCandidate
	core.votesReceived[core.nodeID] = struct{}{}

	err = core.HandleRequestVoteResponse(RequestVoteResponse{
		ResponderID: "node-2",
		Term:        2,
		VoteGranted: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	if core.role != RaftLeader {
		t.Errorf("role = %v, want %v", core.role, RaftLeader)
	}

	if len(core.votesReceived) != 2 {
		t.Errorf("votesReceived length = %d, want 2", len(core.votesReceived))
	}
}

func TestHandleRequestVoteResponseDuplicateVoteNotCountedTwice(t *testing.T) {
	config := Config{
		NodeID: "node-1",
		Members: []NodeID{
			"node-1",
			"node-2",
			"node-3",
			"node-4",
			"node-5",
		},
	}
	storage := &testStorage{
		hardState: HardState{
			CurrentTerm: 2,
			VotedFor:    "node-1",
		},
	}

	core, err := NewRaftCore(config, storage)
	if err != nil {
		t.Fatal(err)
	}

	core.role = RaftCandidate
	core.votesReceived[core.nodeID] = struct{}{}

	resp := RequestVoteResponse{
		ResponderID: "node-2",
		Term:        2,
		VoteGranted: true,
	}

	if err := core.HandleRequestVoteResponse(resp); err != nil {
		t.Fatal(err)
	}

	if err := core.HandleRequestVoteResponse(resp); err != nil {
		t.Fatal(err)
	}

	if len(core.votesReceived) != 2 {
		t.Errorf("votesReceived length = %d, want 2", len(core.votesReceived))
	}

	if core.role != RaftCandidate {
		t.Errorf("role = %v, want %v", core.role, RaftCandidate)
	}
}

func TestHandleRequestVoteResponseIgnoresOlderTerm(t *testing.T) {
	config := Config{
		NodeID:  "node-1",
		Members: []NodeID{"node-1", "node-2", "node-3"},
	}
	storage := &testStorage{
		hardState: HardState{
			CurrentTerm: 2,
			VotedFor:    "node-1",
		},
	}

	core, err := NewRaftCore(config, storage)
	if err != nil {
		t.Fatal(err)
	}

	core.role = RaftCandidate
	core.votesReceived[core.nodeID] = struct{}{}

	err = core.HandleRequestVoteResponse(RequestVoteResponse{
		ResponderID: "node-2",
		Term:        1,
		VoteGranted: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	if core.role != RaftCandidate {
		t.Errorf("role = %v, want %v", core.role, RaftCandidate)
	}

	if core.currentTerm != 2 {
		t.Errorf("currentTerm = %d, want 2", core.currentTerm)
	}

	if len(core.votesReceived) != 1 {
		t.Errorf("votesReceived length = %d, want 1", len(core.votesReceived))
	}
}

// --- HandleAppendEntries ---

func TestHandleAppendEntriesRejectsOlderTerm(t *testing.T) {
	config := Config{
		NodeID:  "node-1",
		Members: []NodeID{"node-1", "node-2", "node-3"},
	}
	storage := &testStorage{
		hardState: HardState{
			CurrentTerm: 2,
			VotedFor:    "node-1",
		},
	}

	core, err := NewRaftCore(config, storage)
	if err != nil {
		t.Fatal(err)
	}

	core.role = RaftCandidate

	resp, err := core.HandleAppendEntries(AppendEntries{
		Term:     1,
		LeaderID: "node-2",
	})
	if err != nil {
		t.Fatal(err)
	}

	if resp.Success {
		t.Error("AppendEntries from older term was accepted")
	}

	if core.currentTerm != 2 {
		t.Errorf("currentTerm = %d, want 2", core.currentTerm)
	}

	if core.role != RaftCandidate {
		t.Errorf("role = %v, want %v", core.role, RaftCandidate)
	}
}

func TestHandleAppendEntriesHigherTermBecomesFollower(t *testing.T) {
	config := Config{
		NodeID:  "node-1",
		Members: []NodeID{"node-1", "node-2", "node-3"},
	}
	storage := &testStorage{
		hardState: HardState{
			CurrentTerm: 2,
			VotedFor:    "node-1",
		},
	}

	core, err := NewRaftCore(config, storage)
	if err != nil {
		t.Fatal(err)
	}

	core.role = RaftCandidate

	resp, err := core.HandleAppendEntries(AppendEntries{
		Term:     3,
		LeaderID: "node-2",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !resp.Success {
		t.Error("AppendEntries from higher term was rejected")
	}

	if core.role != RaftFollower {
		t.Errorf("role = %v, want %v", core.role, RaftFollower)
	}

	if core.currentTerm != 3 {
		t.Errorf("currentTerm = %d, want 3", core.currentTerm)
	}

	if core.votedFor != "" {
		t.Errorf("votedFor = %q, want empty", core.votedFor)
	}
}

func TestHandleAppendEntriesCurrentTermBecomesFollower(t *testing.T) {
	config := Config{
		NodeID:  "node-1",
		Members: []NodeID{"node-1", "node-2", "node-3"},
	}
	storage := &testStorage{
		hardState: HardState{
			CurrentTerm: 2,
			VotedFor:    "node-1",
		},
	}

	core, err := NewRaftCore(config, storage)
	if err != nil {
		t.Fatal(err)
	}

	core.role = RaftCandidate

	resp, err := core.HandleAppendEntries(AppendEntries{
		Term:     2,
		LeaderID: "node-2",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !resp.Success {
		t.Error("AppendEntries from current term was rejected")
	}

	if core.role != RaftFollower {
		t.Errorf("role = %v, want %v", core.role, RaftFollower)
	}

	if core.currentTerm != 2 {
		t.Errorf("currentTerm = %d, want 2", core.currentTerm)
	}

	if core.votedFor != "node-1" {
		t.Errorf("votedFor = %q, want %q", core.votedFor, "node-1")
	}
}
