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

// --- Stage 3 election scenarios ---

func newElectionScenarioCore(t *testing.T, id NodeID, members []NodeID) (*RaftCore, *testStorage) {
	t.Helper()
	storage := &testStorage{}
	core, err := NewRaftCore(Config{NodeID: id, Members: members}, storage)
	if err != nil {
		t.Fatal(err)
	}
	return core, storage
}

func TestCandidateWithoutMajorityRemainsCandidate(t *testing.T) {
	members := []NodeID{"node-1", "node-2", "node-3"}
	core, storage := newElectionScenarioCore(t, "node-1", members)

	req, err := core.StartElection()
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range members[1:] {
		if err := core.HandleRequestVoteResponse(RequestVoteResponse{
			ResponderID: id,
			Term:        req.Term,
			VoteGranted: false,
		}); err != nil {
			t.Fatal(err)
		}
	}

	if core.role != RaftCandidate {
		t.Errorf("role = %v, want %v", core.role, RaftCandidate)
	}
	if len(core.votesReceived) != 1 {
		t.Errorf("votesReceived length = %d, want 1", len(core.votesReceived))
	}
	if storage.hardState != (HardState{CurrentTerm: req.Term, VotedFor: "node-1"}) {
		t.Errorf("hardState = %#v, want self-vote in term %d", storage.hardState, req.Term)
	}
}

func TestTwoCandidatesCompeteInSameTerm(t *testing.T) {
	members := []NodeID{"node-1", "node-2", "node-3"}
	first, _ := newElectionScenarioCore(t, "node-1", members)
	second, _ := newElectionScenarioCore(t, "node-2", members)
	voter, voterStorage := newElectionScenarioCore(t, "node-3", members)

	firstReq, err := first.StartElection()
	if err != nil {
		t.Fatal(err)
	}
	secondReq, err := second.StartElection()
	if err != nil {
		t.Fatal(err)
	}
	if firstReq.Term != secondReq.Term {
		t.Fatalf("candidate terms = %d and %d, want equal", firstReq.Term, secondReq.Term)
	}

	firstResponse, err := first.HandleVoteRequest(secondReq)
	if err != nil {
		t.Fatal(err)
	}
	secondResponse, err := second.HandleVoteRequest(firstReq)
	if err != nil {
		t.Fatal(err)
	}
	voteForFirst, err := voter.HandleVoteRequest(firstReq)
	if err != nil {
		t.Fatal(err)
	}
	voteForSecond, err := voter.HandleVoteRequest(secondReq)
	if err != nil {
		t.Fatal(err)
	}
	if firstResponse.VoteGranted || secondResponse.VoteGranted || !voteForFirst.VoteGranted || voteForSecond.VoteGranted {
		t.Fatalf("same-term votes: first=%#v, second=%#v, voter for first=%#v, voter for second=%#v",
			firstResponse, secondResponse, voteForFirst, voteForSecond)
	}

	for _, resp := range []RequestVoteResponse{firstResponse, voteForSecond} {
		if err := second.HandleRequestVoteResponse(resp); err != nil {
			t.Fatal(err)
		}
	}
	if err := first.HandleRequestVoteResponse(secondResponse); err != nil {
		t.Fatal(err)
	}
	if err := first.HandleRequestVoteResponse(voteForFirst); err != nil {
		t.Fatal(err)
	}
	if first.role != RaftLeader || second.role != RaftCandidate {
		t.Errorf("roles = (%v, %v), want (leader, candidate)", first.role, second.role)
	}
	if len(second.votesReceived) != 1 {
		t.Errorf("second candidate votesReceived length = %d, want 1", len(second.votesReceived))
	}
	if voterStorage.hardState != (HardState{CurrentTerm: firstReq.Term, VotedFor: "node-1"}) {
		t.Errorf("voter hardState = %#v, want vote for node-1 in term %d", voterStorage.hardState, firstReq.Term)
	}
}

func TestDuplicateRequestVoteKeepsSingleChoice(t *testing.T) {
	members := []NodeID{"node-1", "node-2", "node-3"}
	voter, storage := newElectionScenarioCore(t, "node-3", members)
	req := RequestVote{Term: 1, CandidateID: "node-1"}
	want := RequestVoteResponse{ResponderID: "node-3", Term: 1, VoteGranted: true}

	for i := 0; i < 2; i++ {
		resp, err := voter.HandleVoteRequest(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp != want {
			t.Errorf("request %d response = %#v, want %#v", i+1, resp, want)
		}
	}

	other, err := voter.HandleVoteRequest(RequestVote{Term: 1, CandidateID: "node-2"})
	if err != nil {
		t.Fatal(err)
	}
	if other.VoteGranted || other.Term != 1 {
		t.Errorf("second candidate response = %#v, want denied in term 1", other)
	}
	if storage.hardState != (HardState{CurrentTerm: 1, VotedFor: "node-1"}) {
		t.Errorf("hardState = %#v, want vote for node-1 in term 1", storage.hardState)
	}

	restarted, err := NewRaftCore(Config{NodeID: "node-3", Members: members}, storage)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := restarted.HandleVoteRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp != want {
		t.Errorf("duplicate after restart response = %#v, want %#v", resp, want)
	}
}

func TestValidHeartbeatStopsCandidateElection(t *testing.T) {
	members := []NodeID{"node-1", "node-2", "node-3"}
	core, storage := newElectionScenarioCore(t, "node-1", members)

	req, err := core.StartElection()
	if err != nil {
		t.Fatal(err)
	}
	// Resetting the election timer belongs to the runtime; the core must stop campaigning.
	heartbeat := AppendEntries{Term: req.Term, LeaderID: "node-2"}
	resp, err := core.HandleAppendEntries(heartbeat)
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Success || resp.Term != req.Term {
		t.Fatalf("heartbeat response = %#v, want success in term %d", resp, req.Term)
	}
	if err := core.HandleRequestVoteResponse(RequestVoteResponse{
		ResponderID: "node-3",
		Term:        req.Term,
		VoteGranted: true,
	}); err != nil {
		t.Fatal(err)
	}

	saves := storage.saveCalls
	resp, err = core.HandleAppendEntries(heartbeat)
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Success || resp.Term != req.Term {
		t.Errorf("repeated heartbeat response = %#v, want success in term %d", resp, req.Term)
	}
	if core.role != RaftFollower || core.currentTerm != req.Term || len(core.votesReceived) != 0 {
		t.Errorf("state after heartbeats: role=%v term=%d votes=%d, want follower in term %d with no pending votes",
			core.role, core.currentTerm, len(core.votesReceived), req.Term)
	}
	if storage.hardState != (HardState{CurrentTerm: req.Term, VotedFor: "node-1"}) {
		t.Errorf("hardState = %#v, want original vote preserved", storage.hardState)
	}
	if storage.saveCalls != saves {
		t.Errorf("repeated heartbeat persisted unchanged state: saveCalls = %d, want %d", storage.saveCalls, saves)
	}
}
