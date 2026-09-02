package raft

type Config struct {
	NodeID  NodeID
	Members []NodeID
}
