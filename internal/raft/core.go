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
	core.currentTerm, core.votedFor = hardState.CurrentTerm, hardState.VotedFor
	return &core, nil
}

// StartElection starts an election, makes the current node a candidate and returns a RequestVote object
func (core *RaftCore) StartElection() (RequestVote, RoleTransition, error) {
	newState := HardState{CurrentTerm: core.currentTerm + 1, VotedFor: core.nodeID}
	if err := core.storage.SaveHardState(newState); err != nil {
		return RequestVote{}, KeepCurrentRole, err
	}

	core.votesReceived = make(map[NodeID]struct{})
	core.currentTerm = newState.CurrentTerm
	core.votedFor = newState.VotedFor
	core.role = RaftCandidate
	core.votesReceived[core.nodeID] = struct{}{}

	// Check if this is a one-node cluster and make the current node a leader
	majority := len(core.members)/2 + 1
	votes := len(core.votesReceived)

	if votes >= majority {
		core.role = RaftLeader
		return RequestVote{}, BecomeLeader, nil
	}

	// TODO: Temporary until logs are implemented
	lastLogIndex, lastLogTerm := core.getLastLogInfo()

	return RequestVote{Term: core.currentTerm, CandidateID: core.nodeID, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}, BecomeCandidate, nil
}

// HandleVoteRequest decides whether the current node will grant a vote to the requester
func (core *RaftCore) HandleVoteRequest(req RequestVote) (RequestVoteResponse, RoleTransition, error) {
	// TODO: Temporary until logs are implemented
	lastLogIndex, lastLogTerm := core.getLastLogInfo()
	// If the node requesting a vote is among the current node's peers (but is not the current node itself), and the current node hasn't voted for anyone in the requested term, the current node grants votes for the requester, we have 2 options:
	//1. The requester's log is at least as up to date as the current node's log -> become a follower and vote for it
	//2. The requester's log is stale but the request is from higher term -> become a follower without voting
	if slices.Contains(core.members, req.CandidateID) && req.CandidateID != core.nodeID && (req.Term > core.currentTerm || (req.Term == core.currentTerm && (core.votedFor == "" || core.votedFor == req.CandidateID))) {
		if req.LastLogTerm > lastLogTerm || (req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex) {
			alreadyFollower, err := core.becomeFollower(req.Term, req.CandidateID)
			if err != nil {
				return RequestVoteResponse{}, KeepCurrentRole, err
			}
			if alreadyFollower {
				return RequestVoteResponse{ResponderID: core.nodeID, Term: core.currentTerm, VoteGranted: true}, KeepCurrentRole, nil
			}
			return RequestVoteResponse{ResponderID: core.nodeID, Term: core.currentTerm, VoteGranted: true}, BecomeFollower, nil
		} else if req.Term > core.currentTerm {
			alreadyFollower, err := core.becomeFollower(req.Term, "")
			if err != nil {
				return RequestVoteResponse{}, KeepCurrentRole, err
			}

			if alreadyFollower {
				return RequestVoteResponse{ResponderID: core.nodeID, Term: core.currentTerm, VoteGranted: false}, KeepCurrentRole, nil
			}
			return RequestVoteResponse{ResponderID: core.nodeID, Term: core.currentTerm, VoteGranted: false}, BecomeFollower, nil
		}
	}
	return RequestVoteResponse{ResponderID: core.nodeID, Term: core.currentTerm, VoteGranted: false}, KeepCurrentRole, nil
}

// HandleRequestVoteResponse handles the received vote-request response
func (core *RaftCore) HandleRequestVoteResponse(resp RequestVoteResponse) (RoleTransition, error) {
	if !slices.Contains(core.members, resp.ResponderID) || resp.Term < core.currentTerm {
		return KeepCurrentRole, nil
	}
	if resp.Term > core.currentTerm {
		alreadyFollower, err := core.becomeFollower(resp.Term, "")
		if err != nil {
			return KeepCurrentRole, err
		}

		if alreadyFollower {
			return KeepCurrentRole, nil
		}
		return BecomeFollower, nil
	}
	if core.role != RaftCandidate {
		return KeepCurrentRole, nil
	}

	if resp.VoteGranted {
		core.votesReceived[resp.ResponderID] = struct{}{}
		majority := len(core.members)/2 + 1
		votes := len(core.votesReceived)

		if votes >= majority {
			core.role = RaftLeader
			return BecomeLeader, nil
		}
	}

	return KeepCurrentRole, nil
}

// TODO: Temporary helper until logs are implemented. Replace with real implementation!
func (core *RaftCore) getLastLogInfo() (LogIndex, Term) {
	return 0, 0
}

// becomeFollower transitions the current node into a follower for a given term and votedFor. It returns a boolean telling whether or not the role changed or it was already a follower
func (core *RaftCore) becomeFollower(term Term, votedFor NodeID) (bool, error) {
	if err := core.storage.SaveHardState(HardState{CurrentTerm: term, VotedFor: votedFor}); err != nil {
		return false, err
	}
	alreadyFollower := core.role == RaftFollower
	core.role = RaftFollower
	core.currentTerm = term
	core.votedFor = votedFor
	core.votesReceived = make(map[NodeID]struct{})

	return alreadyFollower, nil
}
