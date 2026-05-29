package server

import (
	"context"
	"sync"
	"time"

	"github.com/juanjuandog/mini-raftkv/internal/pb"
	"github.com/juanjuandog/mini-raftkv/internal/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GRPCTransport struct {
	mu    sync.Mutex
	peers map[string]string
	conns map[string]*grpc.ClientConn
}

func NewGRPCTransport(peers map[string]string) *GRPCTransport {
	return &GRPCTransport{peers: peers, conns: map[string]*grpc.ClientConn{}}
}

func (t *GRPCTransport) RequestVote(peerID string, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	client, err := t.client(peerID)
	if err != nil {
		return raft.RequestVoteResponse{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := client.RequestVote(ctx, &pb.RequestVoteRequest{
		Term: req.Term, CandidateId: req.CandidateID, LastLogIndex: req.LastLogIndex, LastLogTerm: req.LastLogTerm,
	})
	if err != nil {
		return raft.RequestVoteResponse{}, err
	}
	return raft.RequestVoteResponse{Term: resp.Term, VoteGranted: resp.VoteGranted}, nil
}

func (t *GRPCTransport) AppendEntries(peerID string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	client, err := t.client(peerID)
	if err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	entries := make([]*pb.LogEntry, 0, len(req.Entries))
	for _, entry := range req.Entries {
		entries = append(entries, FromRaftEntry(entry))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := client.AppendEntries(ctx, &pb.AppendEntriesRequest{
		Term: req.Term, LeaderId: req.LeaderID, PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm: req.PrevLogTerm, Entries: entries, LeaderCommit: req.LeaderCommit,
	})
	if err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	return raft.AppendEntriesResponse{Term: resp.Term, Success: resp.Success, MatchIndex: resp.MatchIndex}, nil
}

func (t *GRPCTransport) InstallSnapshot(peerID string, req raft.InstallSnapshotRequest) (raft.InstallSnapshotResponse, error) {
	client, err := t.client(peerID)
	if err != nil {
		return raft.InstallSnapshotResponse{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.InstallSnapshot(ctx, &pb.InstallSnapshotRequest{
		Term: req.Term, LeaderId: req.LeaderID, LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm: req.LastIncludedTerm, Data: req.Data,
	})
	if err != nil {
		return raft.InstallSnapshotResponse{}, err
	}
	return raft.InstallSnapshotResponse{Term: resp.Term}, nil
}

func (t *GRPCTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	var firstErr error
	for peerID, conn := range t.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(t.conns, peerID)
	}
	return firstErr
}

func (t *GRPCTransport) client(peerID string) (pb.RaftKVClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if conn := t.conns[peerID]; conn != nil {
		return pb.NewRaftKVClient(conn), nil
	}
	addr := t.peers[peerID]
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	t.conns[peerID] = conn
	return pb.NewRaftKVClient(conn), nil
}
