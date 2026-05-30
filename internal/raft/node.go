package raft

import (
	"math/rand"
	"sync"
	"time"
)

type Node struct {
	mu sync.Mutex

	id        string
	peers     map[string]string
	storage   StableStore
	transport Transport
	metrics   *Metrics

	state             State
	leaderID          string
	currentTerm       uint64
	votedFor          string
	log               []LogEntry
	commitIndex       uint64
	lastApplied       uint64
	snapshotIndex     uint64
	snapshotTerm      uint64
	lastSnapshotState []byte

	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	electionBaseTicks   int
	electionJitterTicks int
	electionTicks       int
	heartbeatTicks      int
	snapshotThreshold   uint64
	elapsed             int
	stopped             bool
	rng                 *rand.Rand

	sm *stateMachine
}

func NewNode(cfg Config) (*Node, error) {
	st, err := cfg.Storage.Load()
	if err != nil {
		return nil, err
	}
	loadedLastApplied := st.LastApplied
	n := &Node{
		id:                  cfg.ID,
		peers:               cfg.Peers,
		storage:             cfg.Storage,
		transport:           cfg.Transport,
		metrics:             cfg.Metrics,
		state:               Follower,
		currentTerm:         st.CurrentTerm,
		votedFor:            st.VotedFor,
		log:                 append([]LogEntry(nil), st.Log...),
		commitIndex:         loadedLastApplied,
		lastApplied:         st.SnapshotIndex,
		snapshotIndex:       st.SnapshotIndex,
		snapshotTerm:        st.SnapshotTerm,
		lastSnapshotState:   append([]byte(nil), st.LastSnapshotState...),
		nextIndex:           map[string]uint64{},
		matchIndex:          map[string]uint64{},
		electionBaseTicks:   cfg.ElectionTicks,
		electionJitterTicks: cfg.ElectionJitterTicks,
		heartbeatTicks:      cfg.HeartbeatTicks,
		snapshotThreshold:   cfg.SnapshotThreshold,
		rng:                 rand.New(rand.NewSource(time.Now().UnixNano() + int64(len(cfg.ID))*7919)),
		sm:                  newStateMachine(),
	}
	if n.electionBaseTicks == 0 {
		n.electionBaseTicks = 5
	}
	if n.electionJitterTicks == 0 && len(n.peers) > 0 {
		n.electionJitterTicks = n.electionBaseTicks
	}
	if n.heartbeatTicks == 0 {
		n.heartbeatTicks = 1
	}
	n.resetElectionTimeoutLocked()
	if err := n.sm.restore(st.LastSnapshotState); err != nil {
		return nil, err
	}
	for _, e := range n.log {
		if e.Index > n.lastApplied && e.Index <= loadedLastApplied {
			n.sm.apply(e.Command)
			n.lastApplied = e.Index
		}
	}
	if n.commitIndex < n.lastApplied {
		n.commitIndex = n.lastApplied
	}
	return n, nil
}

func (n *Node) ID() string {
	return n.id
}

func (n *Node) State() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

func (n *Node) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state == Leader {
		return n.id
	}
	return n.leaderID
}

func (n *Node) Status() NodeStatus {
	n.mu.Lock()
	defer n.mu.Unlock()
	leaderID := n.leaderID
	if n.state == Leader {
		leaderID = n.id
	}
	peers := make(map[string]string, len(n.peers))
	for id, addr := range n.peers {
		peers[id] = addr
	}
	nextIndex := make(map[string]uint64, len(n.nextIndex))
	for id, idx := range n.nextIndex {
		nextIndex[id] = idx
	}
	matchIndex := make(map[string]uint64, len(n.matchIndex))
	for id, idx := range n.matchIndex {
		matchIndex[id] = idx
	}
	return NodeStatus{
		ID:                  n.id,
		State:               n.state,
		LeaderID:            leaderID,
		CurrentTerm:         n.currentTerm,
		VotedFor:            n.votedFor,
		CommitIndex:         n.commitIndex,
		LastApplied:         n.lastApplied,
		LastLogIndex:        n.lastIndexLocked(),
		LastLogTerm:         n.lastLogTermLocked(),
		SnapshotIndex:       n.snapshotIndex,
		SnapshotTerm:        n.snapshotTerm,
		LogLength:           len(n.log),
		Peers:               peers,
		NextIndex:           nextIndex,
		MatchIndex:          matchIndex,
		ElectionTicks:       n.electionTicks,
		ElectionElapsed:     n.elapsed,
		HeartbeatTicks:      n.heartbeatTicks,
		SnapshotThreshold:   n.snapshotThreshold,
		Stopped:             n.stopped,
		ElectionJitterTicks: n.electionJitterTicks,
	}
}

func (n *Node) Tick() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.elapsed++
	if n.state == Leader {
		if n.elapsed >= n.heartbeatTicks {
			n.elapsed = 0
			n.mu.Unlock()
			n.replicateAll()
			return
		}
		n.mu.Unlock()
		return
	}
	if n.elapsed < n.electionTicks {
		n.mu.Unlock()
		return
	}
	n.elapsed = 0
	n.mu.Unlock()
	n.startElection()
}

func (n *Node) Get(key string) (string, bool, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	v, ok := n.sm.get(key)
	return v, ok, nil
}

func (n *Node) LinearizableGet(key string) (string, bool, error) {
	if !n.confirmLeadership() {
		return "", false, ErrNoQuorum
	}
	return n.Get(key)
}

func (n *Node) Put(key, value, clientID string, requestID uint64) error {
	_, err := n.propose(Command{Op: OpPut, Key: key, Value: value, ClientID: clientID, RequestID: requestID})
	return err
}

func (n *Node) Delete(key, clientID string, requestID uint64) error {
	_, err := n.propose(Command{Op: OpDelete, Key: key, ClientID: clientID, RequestID: requestID})
	return err
}

func (n *Node) Stop() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.stopped = true
	return n.storage.Close()
}

func (n *Node) RequestVote(req RequestVoteRequest) RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return RequestVoteResponse{Term: n.currentTerm}
	}
	if req.Term < n.currentTerm {
		return RequestVoteResponse{Term: n.currentTerm}
	}
	if req.Term > n.currentTerm {
		n.becomeFollowerLocked(req.Term)
	}
	upToDate := req.LastLogTerm > n.lastLogTermLocked() ||
		(req.LastLogTerm == n.lastLogTermLocked() && req.LastLogIndex >= n.lastIndexLocked())
	granted := upToDate && (n.votedFor == "" || n.votedFor == req.CandidateID)
	if granted {
		n.votedFor = req.CandidateID
		n.elapsed = 0
		n.resetElectionTimeoutLocked()
		_ = n.persistLocked()
	}
	return RequestVoteResponse{Term: n.currentTerm, VoteGranted: granted}
}

func (n *Node) AppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return AppendEntriesResponse{Term: n.currentTerm}
	}
	if req.Term < n.currentTerm {
		return AppendEntriesResponse{Term: n.currentTerm}
	}
	if req.Term > n.currentTerm || n.state != Follower {
		n.becomeFollowerLocked(req.Term)
	}
	n.elapsed = 0
	n.resetElectionTimeoutLocked()
	n.leaderID = req.LeaderID
	if req.PrevLogIndex > n.lastIndexLocked() || n.termAtLocked(req.PrevLogIndex) != req.PrevLogTerm {
		return AppendEntriesResponse{Term: n.currentTerm}
	}
	for _, entry := range req.Entries {
		if entry.Index <= n.snapshotIndex {
			continue
		}
		if entry.Index <= n.lastIndexLocked() && n.termAtLocked(entry.Index) != entry.Term {
			n.truncateFromLocked(entry.Index)
		}
		if entry.Index > n.lastIndexLocked() {
			n.log = append(n.log, entry)
		}
	}
	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = min(req.LeaderCommit, n.lastIndexLocked())
		n.applyLocked()
	}
	_ = n.persistLocked()
	return AppendEntriesResponse{Term: n.currentTerm, Success: true, MatchIndex: n.lastIndexLocked()}
}

func (n *Node) InstallSnapshot(req InstallSnapshotRequest) InstallSnapshotResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return InstallSnapshotResponse{Term: n.currentTerm}
	}
	if req.Term < n.currentTerm {
		return InstallSnapshotResponse{Term: n.currentTerm}
	}
	if req.Term > n.currentTerm || n.state != Follower {
		n.becomeFollowerLocked(req.Term)
	}
	n.elapsed = 0
	n.resetElectionTimeoutLocked()
	n.leaderID = req.LeaderID
	n.snapshotIndex = req.LastIncludedIndex
	n.snapshotTerm = req.LastIncludedTerm
	n.lastSnapshotState = append([]byte(nil), req.Data...)
	n.log = entriesAfter(n.log, req.LastIncludedIndex)
	n.commitIndex = max(n.commitIndex, req.LastIncludedIndex)
	n.lastApplied = max(n.lastApplied, req.LastIncludedIndex)
	n.sm = newStateMachine()
	_ = n.sm.restore(req.Data)
	_ = n.persistLocked()
	return InstallSnapshotResponse{Term: n.currentTerm}
}

func (n *Node) propose(cmd Command) (uint64, error) {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return 0, ErrStopped
	}
	if n.state != Leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	if cmd.ClientID != "" && n.sm.seen[cmd.ClientID] >= cmd.RequestID {
		n.mu.Unlock()
		return n.lastApplied, nil
	}
	entry := LogEntry{Index: n.lastIndexLocked() + 1, Term: n.currentTerm, Command: cmd}
	n.log = append(n.log, entry)
	_ = n.persistLocked()
	n.mu.Unlock()

	n.replicateUntilCommitted(entry.Index)
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.commitIndex < entry.Index || n.state != Leader {
		return entry.Index, ErrNoQuorum
	}
	return entry.Index, nil
}

func (n *Node) startElection() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.state = Candidate
	n.leaderID = ""
	n.currentTerm++
	n.votedFor = n.id
	n.elapsed = 0
	n.resetElectionTimeoutLocked()
	term := n.currentTerm
	lastIndex := n.lastIndexLocked()
	lastTerm := n.lastLogTermLocked()
	_ = n.persistLocked()
	n.mu.Unlock()

	votes := 1
	for peerID := range n.peers {
		resp, err := n.transport.RequestVote(peerID, RequestVoteRequest{
			Term: term, CandidateID: n.id, LastLogIndex: lastIndex, LastLogTerm: lastTerm,
		})
		if err != nil {
			continue
		}
		if resp.Term > term {
			n.mu.Lock()
			n.becomeFollowerLocked(resp.Term)
			n.mu.Unlock()
			return
		}
		if resp.VoteGranted {
			votes++
		}
	}
	if votes >= n.quorum() {
		n.mu.Lock()
		if n.currentTerm == term && n.state == Candidate {
			n.becomeLeaderLocked()
		}
		n.mu.Unlock()
		n.replicateAll()
	}
}

func (n *Node) replicateUntilCommitted(index uint64) {
	for i := 0; i < 64; i++ {
		n.replicateAll()
		n.mu.Lock()
		done := n.commitIndex >= index || n.state != Leader
		n.mu.Unlock()
		if done {
			n.replicateAll()
			return
		}
	}
}

func (n *Node) replicateAll() {
	n.mu.Lock()
	if n.state != Leader || n.stopped {
		n.mu.Unlock()
		return
	}
	peers := make([]string, 0, len(n.peers))
	for peerID := range n.peers {
		peers = append(peers, peerID)
	}
	n.mu.Unlock()

	for _, peerID := range peers {
		n.replicateOne(peerID)
	}
	n.mu.Lock()
	n.advanceCommitLocked()
	n.applyLocked()
	_ = n.persistLocked()
	n.mu.Unlock()
}

func (n *Node) confirmLeadership() bool {
	n.mu.Lock()
	if n.stopped || n.state != Leader {
		n.mu.Unlock()
		return false
	}
	term := n.currentTerm
	prevIndex := n.lastIndexLocked()
	prevTerm := n.termAtLocked(prevIndex)
	leaderCommit := n.commitIndex
	peers := make([]string, 0, len(n.peers))
	for peerID := range n.peers {
		peers = append(peers, peerID)
	}
	n.mu.Unlock()

	acks := 1
	for _, peerID := range peers {
		resp, err := n.transport.AppendEntries(peerID, AppendEntriesRequest{
			Term: term, LeaderID: n.id, PrevLogIndex: prevIndex, PrevLogTerm: prevTerm, LeaderCommit: leaderCommit,
		})
		if err != nil {
			continue
		}
		if resp.Term > term {
			n.mu.Lock()
			n.becomeFollowerLocked(resp.Term)
			n.mu.Unlock()
			return false
		}
		if resp.Success {
			acks++
		}
	}
	return acks >= n.quorum()
}

func (n *Node) replicateOne(peerID string) {
	for attempts := 0; attempts < 64; attempts++ {
		n.mu.Lock()
		if n.state != Leader {
			n.mu.Unlock()
			return
		}
		next := n.nextIndex[peerID]
		if next == 0 {
			next = n.lastIndexLocked() + 1
			n.nextIndex[peerID] = next
		}
		if next <= n.snapshotIndex {
			req := InstallSnapshotRequest{
				Term: n.currentTerm, LeaderID: n.id, LastIncludedIndex: n.snapshotIndex,
				LastIncludedTerm: n.snapshotTerm, Data: append([]byte(nil), n.lastSnapshotState...),
			}
			n.mu.Unlock()
			resp, err := n.transport.InstallSnapshot(peerID, req)
			n.mu.Lock()
			if err == nil && resp.Term <= n.currentTerm {
				n.nextIndex[peerID] = n.snapshotIndex + 1
				n.matchIndex[peerID] = n.snapshotIndex
			}
			if err != nil && n.metrics != nil {
				n.metrics.ReplicationErrors.Inc()
			}
			n.mu.Unlock()
			return
		}
		prev := next - 1
		req := AppendEntriesRequest{
			Term: n.currentTerm, LeaderID: n.id, PrevLogIndex: prev, PrevLogTerm: n.termAtLocked(prev),
			Entries: append([]LogEntry(nil), n.entriesFromLocked(next)...), LeaderCommit: n.commitIndex,
		}
		n.mu.Unlock()

		resp, err := n.transport.AppendEntries(peerID, req)
		n.mu.Lock()
		if err != nil {
			if n.metrics != nil {
				n.metrics.ReplicationErrors.Inc()
			}
			n.mu.Unlock()
			return
		}
		if resp.Term > n.currentTerm {
			n.becomeFollowerLocked(resp.Term)
			n.mu.Unlock()
			return
		}
		if resp.Success {
			n.matchIndex[peerID] = resp.MatchIndex
			n.nextIndex[peerID] = resp.MatchIndex + 1
			n.mu.Unlock()
			return
		}
		if n.nextIndex[peerID] > 1 {
			n.nextIndex[peerID]--
		}
		n.mu.Unlock()
	}
}

func (n *Node) becomeFollowerLocked(term uint64) {
	n.state = Follower
	n.leaderID = ""
	n.currentTerm = term
	n.votedFor = ""
	n.elapsed = 0
	n.resetElectionTimeoutLocked()
	if n.metrics != nil {
		n.metrics.RoleChanges.Inc()
	}
	_ = n.persistLocked()
}

func (n *Node) becomeLeaderLocked() {
	n.state = Leader
	n.leaderID = n.id
	last := n.lastIndexLocked()
	for peerID := range n.peers {
		n.nextIndex[peerID] = last + 1
		n.matchIndex[peerID] = 0
	}
	if n.metrics != nil {
		n.metrics.RoleChanges.Inc()
	}
}

func (n *Node) advanceCommitLocked() {
	for idx := n.lastIndexLocked(); idx > n.commitIndex; idx-- {
		if n.termAtLocked(idx) != n.currentTerm {
			continue
		}
		count := 1
		for _, match := range n.matchIndex {
			if match >= idx {
				count++
			}
		}
		if count >= n.quorum() {
			n.commitIndex = idx
			if n.metrics != nil {
				n.metrics.CommittedEntries.Inc()
			}
			return
		}
	}
}

func (n *Node) applyLocked() {
	for n.lastApplied < n.commitIndex {
		next := n.lastApplied + 1
		entry, ok := n.entryAtLocked(next)
		if !ok {
			n.lastApplied = next
			continue
		}
		n.sm.apply(entry.Command)
		n.lastApplied = next
		if n.metrics != nil {
			n.metrics.AppliedEntries.Inc()
		}
	}
	if n.snapshotThreshold > 0 && n.lastApplied > n.snapshotIndex && n.lastApplied-n.snapshotIndex >= n.snapshotThreshold {
		raw, err := n.sm.snapshot()
		if err == nil {
			n.snapshotIndex = n.lastApplied
			n.snapshotTerm = n.termAtLocked(n.lastApplied)
			n.lastSnapshotState = raw
			n.log = entriesAfter(n.log, n.snapshotIndex)
			if n.metrics != nil {
				n.metrics.SnapshotsCreated.Inc()
			}
		}
	}
}

func (n *Node) persistLocked() error {
	return n.storage.Save(PersistedState{
		CurrentTerm:       n.currentTerm,
		VotedFor:          n.votedFor,
		Log:               n.log,
		SnapshotIndex:     n.snapshotIndex,
		SnapshotTerm:      n.snapshotTerm,
		LastApplied:       n.lastApplied,
		LastSnapshotState: n.lastSnapshotState,
	})
}

func (n *Node) lastIndexLocked() uint64 {
	if len(n.log) == 0 {
		return n.snapshotIndex
	}
	return n.log[len(n.log)-1].Index
}

func (n *Node) lastLogTermLocked() uint64 {
	return n.termAtLocked(n.lastIndexLocked())
}

func (n *Node) termAtLocked(index uint64) uint64 {
	if index == 0 {
		return 0
	}
	if index == n.snapshotIndex {
		return n.snapshotTerm
	}
	if index < n.snapshotIndex {
		return 0
	}
	offset := index - n.snapshotIndex - 1
	if offset >= uint64(len(n.log)) {
		return 0
	}
	return n.log[offset].Term
}

func (n *Node) entryAtLocked(index uint64) (LogEntry, bool) {
	if index <= n.snapshotIndex {
		return LogEntry{}, false
	}
	offset := index - n.snapshotIndex - 1
	if offset >= uint64(len(n.log)) {
		return LogEntry{}, false
	}
	return n.log[offset], true
}

func (n *Node) entriesFromLocked(index uint64) []LogEntry {
	if index > n.lastIndexLocked() {
		return nil
	}
	if index <= n.snapshotIndex {
		index = n.snapshotIndex + 1
	}
	offset := index - n.snapshotIndex - 1
	return n.log[offset:]
}

func (n *Node) truncateFromLocked(index uint64) {
	if index <= n.snapshotIndex {
		n.log = nil
		return
	}
	offset := index - n.snapshotIndex - 1
	if offset < uint64(len(n.log)) {
		n.log = n.log[:offset]
	}
}

func (n *Node) quorum() int {
	return (len(n.peers)+1)/2 + 1
}

func (n *Node) resetElectionTimeoutLocked() {
	n.electionTicks = n.electionBaseTicks
	if n.electionJitterTicks > 0 {
		n.electionTicks += n.rng.Intn(n.electionJitterTicks + 1)
	}
}

func entriesAfter(entries []LogEntry, index uint64) []LogEntry {
	for i, entry := range entries {
		if entry.Index > index {
			return append([]LogEntry(nil), entries[i:]...)
		}
	}
	return nil
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
