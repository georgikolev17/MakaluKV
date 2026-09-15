package raft

import (
	"errors"
	"testing"
)

func TestNewRaftCoreValidation(t *testing.T) {
	config := Config{NodeID: "node-1", Members: []NodeID{}}
	storage := &testStorage{}
	_, err := NewRaftCore(config, storage)

	// Test that empty members array throws an error
	if err == nil {
		t.Error("Initialising core with empty members array should throw an error but it does not")
	} else if err.Error() != "members cannot be empty during configuration" {
		t.Errorf("Expcted error: %v, got: %v", "members cannot be empty during configuration", err.Error())
	}

	// Test empty member ID throws an error
	config.NodeID = "node-1"
	config.Members = []NodeID{"node-1", "node-2", ""}
	_, err = NewRaftCore(config, storage)
	if err == nil {
		t.Error("Initialising core with empty member id should throw an error but it does not")
	} else if err.Error() != "members cannot have empty ids" {
		t.Errorf("Expcted error: %v, got: %v", "members cannot have empty ids", err.Error())
	}

	// Test NodeID not in members throws an error
	config.NodeID = "node-4"
	config.Members = []NodeID{"node-1", "node-2", "node-3"}
	_, err = NewRaftCore(config, storage)
	if err == nil {
		t.Error("Initialising core with NodeID not in members IDs should throw an error but it does not.")
	} else if err.Error() != "NodeID was not found in members" {
		t.Errorf("Expcted error: %v, got: %v", "NodeID was not found in members", err.Error())
	}
}

func TestNewRaftCoreStorageLoadFailure(t *testing.T) {
	config := Config{
		NodeID:  "node-1",
		Members: []NodeID{"node-1", "node-2", "node-3"},
	}

	storage := &testStorage{
		loadErr: errors.New("load failure"),
	}

	core, err := NewRaftCore(config, storage)

	if err == nil {
		t.Fatal("NewRaftCore() expected error, got nil")
	}

	if core != nil {
		t.Error("NewRaftCore() returned non-nil core on storage load failure")
	}
}

func TestNewRaftCoreWhenVotedForIsNotInMembers(t *testing.T) {
	config := Config{NodeID: "node-1", Members: []NodeID{"node-1", "node-2", "node-3"}}
	storage := &testStorage{
		hardState: HardState{
			CurrentTerm: 2,
			VotedFor:    "node-22",
		},
	}

	_, err := NewRaftCore(config, storage)

	if err == nil {
		t.Fatal("expected error since votedFor is not in members")
	}
	if err.Error() != "persisted votedFor is not a cluster member" {
		t.Errorf("received error: %q, want: %q", err.Error(), "persisted votedFor is not a cluster member")
	}
}

func TestNewRaftCoreCorrectInitialisation(t *testing.T) {
	config := Config{NodeID: "node-1", Members: []NodeID{"node-1", "node-2", "node-3"}}
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

	if core.nodeID != "node-1" {
		t.Errorf("expected nodeID: %q, got: %q", "node-1", core.nodeID)
	}

	if core.currentTerm != storage.hardState.CurrentTerm {
		t.Errorf("expected current term: %d, got: %d", 2, core.currentTerm)
	}

	if core.votedFor != "node-2" {
		t.Errorf("expected votedFor: %q, got: %q", "node-2", core.votedFor)
	}

	if core.role != RaftFollower {
		t.Error("a node was not initialised in a Follower role")
	}

	if core.commitIndex != 0 {
		t.Errorf("commitIndex = %d, want 0", core.commitIndex)
	}

	if core.lastApplied != 0 {
		t.Errorf("lastApplied = %d, want 0", core.lastApplied)
	}

	if len(core.votesReceived) != 0 {
		t.Errorf("votesReceived length = %d, want 0", len(core.votesReceived))
	}
}

func TestNewRaftCoreClonesMembers(t *testing.T) {
	// If the core does not clone the members list, it creates a security issue that it can be modified from the outside
	config := Config{NodeID: "node-1", Members: []NodeID{"node-1", "node-2", "node-3"}}
	storage := &testStorage{}

	core, err := NewRaftCore(config, storage)

	if err != nil {
		t.Fatal(err)
	}

	config.Members[2] = "node-4"
	if core.members[2] == "node-4" {
		t.Errorf("raft core initialisation does not clone the members slice but stores a reference")
	}
}
