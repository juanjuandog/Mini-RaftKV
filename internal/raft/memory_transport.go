package raft

import (
	"errors"
	"sync"
)

type MemoryTransport struct {
	mu      sync.RWMutex
	nodes   map[string]*Node
	blocked map[string]bool
}

func NewMemoryTransport() *MemoryTransport {
	return &MemoryTransport{nodes: map[string]*Node{}, blocked: map[string]bool{}}
}

func (t *MemoryTransport) Register(n *Node) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[n.ID()] = n
}

func (t *MemoryTransport) Block(peerID string, blocked bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.blocked[peerID] = blocked
}

func (t *MemoryTransport) RequestVote(peerID string, req RequestVoteRequest) (RequestVoteResponse, error) {
	n, err := t.node(peerID)
	if err != nil {
		return RequestVoteResponse{}, err
	}
	return n.RequestVote(req), nil
}

func (t *MemoryTransport) AppendEntries(peerID string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	n, err := t.node(peerID)
	if err != nil {
		return AppendEntriesResponse{}, err
	}
	return n.AppendEntries(req), nil
}

func (t *MemoryTransport) InstallSnapshot(peerID string, req InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	n, err := t.node(peerID)
	if err != nil {
		return InstallSnapshotResponse{}, err
	}
	return n.InstallSnapshot(req), nil
}

func (t *MemoryTransport) node(peerID string) (*Node, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.blocked[peerID] {
		return nil, errors.New("peer blocked")
	}
	n := t.nodes[peerID]
	if n == nil {
		return nil, errors.New("peer not found")
	}
	return n, nil
}
