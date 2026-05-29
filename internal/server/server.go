package server

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/juanjuandog/mini-raftkv/internal/pb"
	"github.com/juanjuandog/mini-raftkv/internal/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Server struct {
	pb.UnimplementedRaftKVServer
	mu    sync.Mutex
	node  *raft.Node
	peers map[string]string
	conns map[string]*grpc.ClientConn
}

func New(node *raft.Node) *Server {
	return NewWithPeers(node, nil)
}

func NewWithPeers(node *raft.Node, peers map[string]string) *Server {
	return &Server{node: node, peers: peers, conns: map[string]*grpc.ClientConn{}}
}

func (s *Server) RequestVote(_ context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	resp := s.node.RequestVote(raft.RequestVoteRequest{
		Term: req.Term, CandidateID: req.CandidateId, LastLogIndex: req.LastLogIndex, LastLogTerm: req.LastLogTerm,
	})
	return &pb.RequestVoteResponse{Term: resp.Term, VoteGranted: resp.VoteGranted}, nil
}

func (s *Server) AppendEntries(_ context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	entries := make([]raft.LogEntry, 0, len(req.Entries))
	for _, entry := range req.Entries {
		entries = append(entries, toRaftEntry(entry))
	}
	resp := s.node.AppendEntries(raft.AppendEntriesRequest{
		Term: req.Term, LeaderID: req.LeaderId, PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm: req.PrevLogTerm, Entries: entries, LeaderCommit: req.LeaderCommit,
	})
	return &pb.AppendEntriesResponse{Term: resp.Term, Success: resp.Success, MatchIndex: resp.MatchIndex}, nil
}

func (s *Server) InstallSnapshot(_ context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	resp := s.node.InstallSnapshot(raft.InstallSnapshotRequest{
		Term: req.Term, LeaderID: req.LeaderId, LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm: req.LastIncludedTerm, Data: req.Data,
	})
	return &pb.InstallSnapshotResponse{Term: resp.Term}, nil
}

func (s *Server) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if s.node.State() != raft.Leader {
		return s.forwardGet(req)
	}
	value, ok, err := s.node.LinearizableGet(req.Key)
	if err != nil {
		return &pb.GetResponse{LeaderId: s.node.LeaderID(), Error: err.Error()}, nil
	}
	return &pb.GetResponse{Found: ok, Value: value, LeaderId: s.node.LeaderID()}, nil
}

func (s *Server) Put(_ context.Context, req *pb.PutRequest) (*pb.MutateResponse, error) {
	err := s.node.Put(req.Key, req.Value, req.ClientId, req.RequestId)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return s.forwardPut(req)
		}
		return mutateError(s.node.LeaderID(), err), nil
	}
	return &pb.MutateResponse{Ok: true, LeaderId: s.node.LeaderID()}, nil
}

func (s *Server) Delete(_ context.Context, req *pb.DeleteRequest) (*pb.MutateResponse, error) {
	err := s.node.Delete(req.Key, req.ClientId, req.RequestId)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return s.forwardDelete(req)
		}
		return mutateError(s.node.LeaderID(), err), nil
	}
	return &pb.MutateResponse{Ok: true, LeaderId: s.node.LeaderID()}, nil
}

func (s *Server) forwardPut(req *pb.PutRequest) (*pb.MutateResponse, error) {
	leaderID := s.node.LeaderID()
	client, closeFn, ok := s.leaderClient(leaderID)
	if !ok {
		return mutateError(leaderID, raft.ErrNotLeader), nil
	}
	defer closeFn()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return client.Put(ctx, req)
}

func (s *Server) forwardDelete(req *pb.DeleteRequest) (*pb.MutateResponse, error) {
	leaderID := s.node.LeaderID()
	client, closeFn, ok := s.leaderClient(leaderID)
	if !ok {
		return mutateError(leaderID, raft.ErrNotLeader), nil
	}
	defer closeFn()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return client.Delete(ctx, req)
}

func (s *Server) forwardGet(req *pb.GetRequest) (*pb.GetResponse, error) {
	leaderID := s.node.LeaderID()
	client, closeFn, ok := s.leaderClient(leaderID)
	if !ok {
		return &pb.GetResponse{LeaderId: leaderID, Error: raft.ErrNotLeader.Error()}, nil
	}
	defer closeFn()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return client.Get(ctx, req)
}

func (s *Server) leaderClient(leaderID string) (pb.RaftKVClient, func(), bool) {
	addr := s.peers[leaderID]
	if leaderID == "" || addr == "" {
		return nil, func() {}, false
	}
	s.mu.Lock()
	if conn := s.conns[leaderID]; conn != nil {
		s.mu.Unlock()
		return pb.NewRaftKVClient(conn), func() {}, true
	}
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		s.mu.Unlock()
		return nil, func() {}, false
	}
	s.conns[leaderID] = conn
	s.mu.Unlock()
	return pb.NewRaftKVClient(conn), func() {}, true
}

func mutateError(leaderID string, err error) *pb.MutateResponse {
	if errors.Is(err, raft.ErrNotLeader) {
		return &pb.MutateResponse{LeaderId: leaderID, Error: err.Error()}
	}
	return &pb.MutateResponse{LeaderId: leaderID, Error: err.Error()}
}

func toRaftEntry(entry *pb.LogEntry) raft.LogEntry {
	return raft.LogEntry{
		Index: entry.Index,
		Term:  entry.Term,
		Command: raft.Command{
			Op: raft.Operation(entry.Op), Key: entry.Key, Value: entry.Value,
			ClientID: entry.ClientId, RequestID: entry.RequestId,
		},
	}
}

func FromRaftEntry(entry raft.LogEntry) *pb.LogEntry {
	return &pb.LogEntry{
		Index: entry.Index, Term: entry.Term, Op: string(entry.Command.Op),
		Key: entry.Command.Key, Value: entry.Command.Value,
		ClientId: entry.Command.ClientID, RequestId: entry.Command.RequestID,
	}
}
