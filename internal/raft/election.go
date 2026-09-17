package raft

import (
	"slices"
)

// StartElection starts an election, makes the current node a candidate and returns a RequestVote object
func (core *RaftCore) StartElection() (RequestVote, error) {
	newState := HardState{CurrentTerm: core.currentTerm + 1, VotedFor: core.nodeID}
	if err := core.storage.SaveHardState(newState); err != nil {
		return RequestVote{}, err
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
		return RequestVote{}, nil
	}

	// TODO: Temporary until logs are implemented
	lastLogIndex, lastLogTerm := core.getLastLogInfo()

	return RequestVote{
		Term:         core.currentTerm,
		CandidateID:  core.nodeID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}, nil
}

// HandleVoteRequest decides whether the current node will grant a vote to the requester
func (core *RaftCore) HandleVoteRequest(req RequestVote) (RequestVoteResponse, error) {
	if !slices.Contains(core.members, req.CandidateID) || req.CandidateID == core.nodeID {
		return RequestVoteResponse{}, nil
	}

	// If the request is from older term, reject it
	if req.Term < core.currentTerm {
		return RequestVoteResponse{
			ResponderID: core.nodeID,
			Term:        core.currentTerm,
			VoteGranted: false,
		}, nil
	}

	// TODO: Temporary until logs are implemented
	lastLogIndex, lastLogTerm := core.getLastLogInfo()

	logUpToDate := req.LastLogTerm > lastLogTerm ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

	if !logUpToDate {
		if req.Term > core.currentTerm {
			err := core.becomeFollower(req.Term, "")
			if err != nil {
				return RequestVoteResponse{}, err
			}
		}
		return RequestVoteResponse{
			ResponderID: core.nodeID,
			Term:        core.currentTerm,
			VoteGranted: false,
		}, nil
	}

	canVote := req.Term > core.currentTerm ||
		(req.Term == core.currentTerm &&
			(core.votedFor == "" || core.votedFor == req.CandidateID))

	if !canVote {
		return RequestVoteResponse{
			ResponderID: core.nodeID,
			Term:        core.currentTerm,
			VoteGranted: false,
		}, nil
	}

	// Everything is correct, and a vote can be granted
	err := core.becomeFollower(req.Term, req.CandidateID)
	if err != nil {
		return RequestVoteResponse{}, err
	}

	return RequestVoteResponse{
		ResponderID: core.nodeID,
		Term:        core.currentTerm,
		VoteGranted: true,
	}, nil
}

// HandleRequestVoteResponse handles the received vote-request response
func (core *RaftCore) HandleRequestVoteResponse(resp RequestVoteResponse) error {
	if !slices.Contains(core.members, resp.ResponderID) || resp.Term < core.currentTerm {
		return nil
	}
	if resp.Term > core.currentTerm {
		err := core.becomeFollower(resp.Term, "")
		if err != nil {
			return err
		}
		return nil
	}
	if core.role != RaftCandidate {
		return nil
	}

	if resp.VoteGranted {
		core.votesReceived[resp.ResponderID] = struct{}{}
		majority := len(core.members)/2 + 1
		votes := len(core.votesReceived)

		if votes >= majority {
			core.role = RaftLeader
			return nil
		}
	}

	return nil
}

// HandleAppendEntries handles incoming AppendEntries messages.
func (core *RaftCore) HandleAppendEntries(appendEntries AppendEntries) (AppendEntriesResponse, error) {
	// TODO: For now, this function handles only empty AppendEntries, just for the purpose of leader management. Implement log handling!!!

	// Validate sender
	if !slices.Contains(core.members, appendEntries.LeaderID) || appendEntries.LeaderID == core.nodeID {
		return AppendEntriesResponse{Success: false, Term: core.currentTerm}, nil
	}

	// Check if this is from old term
	if appendEntries.Term < core.currentTerm {
		return AppendEntriesResponse{Success: false, Term: core.currentTerm}, nil
	}

	if appendEntries.Term > core.currentTerm {
		err := core.becomeFollower(appendEntries.Term, "")
		if err != nil {
			return AppendEntriesResponse{Success: false, Term: core.currentTerm}, err
		}
	} else if core.role != RaftFollower { // If the core is in the same turn as AppendEntries but is not a follower
		err := core.becomeFollower(appendEntries.Term, core.votedFor)

		if err != nil {
			return AppendEntriesResponse{Success: false, Term: core.currentTerm}, err
		}
	}

	// At this point, the sender is valid, the request term is not stale,
	// and the core is a follower. Process the log portion of AppendEntries.
	// TODO: Add log handling here

	return AppendEntriesResponse{Success: true, Term: core.currentTerm}, nil
}
