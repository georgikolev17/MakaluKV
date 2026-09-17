package raft

import (
	"errors"
	"slices"
)

type RaftCore struct {
	nodeID        NodeID
	role          RaftRole
	storage       StableStorage
	currentTerm   Term
	votedFor      NodeID
	commitIndex   LogIndex
	lastApplied   LogIndex
	votesReceived map[NodeID]struct{}
	members       []NodeID
}

// NewRaftCore initialises a Raft core for a given stable storage and configuration
func NewRaftCore(config Config, storage StableStorage) (*RaftCore, error) {
	// Validate config
	if len(config.Members) == 0 {
		return nil, errors.New("members cannot be empty during configuration")
	}
	nodeIDinMembers := false
	for _, member := range config.Members {
		if member == "" {
			return nil, errors.New("members cannot have empty ids")
		}
		if member == config.NodeID {
			nodeIDinMembers = true
		}
	}
	if !nodeIDinMembers {
		return nil, errors.New("NodeID was not found in members")
	}

	// Initialise the raft core
	core := RaftCore{
		storage:       storage,
		nodeID:        config.NodeID,
		role:          RaftFollower,
		members:       slices.Clone(config.Members),
		votesReceived: make(map[NodeID]struct{}),
	}

	hardState, err := core.storage.LoadHardState()
	if err != nil {
		return nil, err
	}

	if hardState.VotedFor != "" && !slices.Contains(core.members, hardState.VotedFor) {
		return nil, errors.New("persisted votedFor is not a cluster member")
	}

	core.currentTerm, core.votedFor = hardState.CurrentTerm, hardState.VotedFor
	return &core, nil
}

// TODO: Temporary helper until logs are implemented. Replace with real implementation!
func (core *RaftCore) getLastLogInfo() (LogIndex, Term) {
	return 0, 0
}

// becomeFollower transitions the current node into a follower for a given term and votedFor. It returns a boolean telling whether or not the role changed or it was already a follower
func (core *RaftCore) becomeFollower(term Term, votedFor NodeID) error {
	if err := core.storage.SaveHardState(HardState{CurrentTerm: term, VotedFor: votedFor}); err != nil {
		return err
	}
	core.role = RaftFollower
	core.currentTerm = term
	core.votedFor = votedFor
	core.votesReceived = make(map[NodeID]struct{})

	return nil
}
