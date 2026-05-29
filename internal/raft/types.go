package raft

import "errors"

type State string

const (
	Follower  State = "Follower"
	Candidate State = "Candidate"
	Leader    State = "Leader"
)

type Operation string

const (
	OpPut    Operation = "put"
	OpDelete Operation = "delete"
)

var (
	ErrNotLeader = errors.New("not leader")
	ErrNotFound  = errors.New("key not found")
	ErrNoQuorum  = errors.New("quorum unavailable")
	ErrStopped   = errors.New("raft node stopped")
)

type Command struct {
	Op        Operation
	Key       string
	Value     string
	ClientID  string
	RequestID uint64
}

type LogEntry struct {
	Index   uint64
	Term    uint64
	Command Command
}

type RequestVoteRequest struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	Term       uint64
	Success    bool
	MatchIndex uint64
}

type InstallSnapshotRequest struct {
	Term              uint64
	LeaderID          string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

type InstallSnapshotResponse struct {
	Term uint64
}

type Transport interface {
	RequestVote(peerID string, req RequestVoteRequest) (RequestVoteResponse, error)
	AppendEntries(peerID string, req AppendEntriesRequest) (AppendEntriesResponse, error)
	InstallSnapshot(peerID string, req InstallSnapshotRequest) (InstallSnapshotResponse, error)
}

type Config struct {
	ID                  string
	Peers               map[string]string
	Storage             StableStore
	Transport           Transport
	ElectionTicks       int
	ElectionJitterTicks int
	HeartbeatTicks      int
	SnapshotThreshold   uint64
	Metrics             *Metrics
}
