package raft

import (
	"path/filepath"
	"testing"
)

func TestLeaderElectionAndReplicatedKV(t *testing.T) {
	nodes, _ := newTestCluster(t, 3, 100)
	leader := electLeader(t, nodes)

	if err := leader.Put("language", "go", "client-a", 1); err != nil {
		t.Fatalf("put through leader: %v", err)
	}

	for _, n := range nodes {
		value, ok, err := n.Get("language")
		if err != nil || !ok || value != "go" {
			t.Fatalf("node %s got value=%q ok=%v err=%v", n.ID(), value, ok, err)
		}
	}

	if err := leader.Delete("language", "client-a", 2); err != nil {
		t.Fatalf("delete through leader: %v", err)
	}
	for _, n := range nodes {
		_, ok, _ := n.Get("language")
		if ok {
			t.Fatalf("node %s still has deleted key", n.ID())
		}
	}
}

func TestElectionTimeoutIsRandomizedWithinConfiguredRange(t *testing.T) {
	node, err := NewNode(Config{
		ID:                  "n-random",
		Peers:               map[string]string{"n2": ""},
		Storage:             NewMemoryStore(),
		Transport:           NewMemoryTransport(),
		ElectionTicks:       10,
		ElectionJitterTicks: 5,
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	seenDifferentTimeout := false
	first := node.electionTicks
	for i := 0; i < 50; i++ {
		node.mu.Lock()
		node.resetElectionTimeoutLocked()
		current := node.electionTicks
		node.mu.Unlock()
		if current < 10 || current > 15 {
			t.Fatalf("election timeout=%d, want in [10,15]", current)
		}
		if current != first {
			seenDifferentTimeout = true
		}
	}
	if !seenDifferentTimeout {
		t.Fatalf("election timeout did not vary across resets")
	}
}

func TestDuplicateClientRequestIsAppliedOnce(t *testing.T) {
	nodes, _ := newTestCluster(t, 3, 100)
	leader := electLeader(t, nodes)

	if err := leader.Put("dup", "first", "client-a", 7); err != nil {
		t.Fatalf("first put: %v", err)
	}
	if err := leader.Put("dup", "second", "client-a", 7); err != nil {
		t.Fatalf("duplicate put: %v", err)
	}

	value, ok, _ := leader.Get("dup")
	if !ok || value != "first" {
		t.Fatalf("duplicate request changed value to %q ok=%v", value, ok)
	}
}

func TestLeaderRepairsFollowerLogConflict(t *testing.T) {
	nodes, _ := newTestCluster(t, 3, 100)
	leader := electLeader(t, nodes)
	follower := firstFollower(nodes)

	follower.mu.Lock()
	follower.log = []LogEntry{{Index: 1, Term: 99, Command: Command{Op: OpPut, Key: "stale", Value: "bad"}}}
	follower.mu.Unlock()

	if err := leader.Put("fresh", "ok", "client-a", 1); err != nil {
		t.Fatalf("put after conflict: %v", err)
	}

	value, ok, _ := follower.Get("fresh")
	if !ok || value != "ok" {
		t.Fatalf("follower conflict was not repaired, value=%q ok=%v", value, ok)
	}
	_, ok, _ = follower.Get("stale")
	if ok {
		t.Fatalf("conflicting stale command survived repair")
	}
}

func TestBoltStoreRestoresCommittedState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.db")
	store, err := OpenBoltStore(path)
	if err != nil {
		t.Fatalf("open bolt: %v", err)
	}
	transport := NewMemoryTransport()
	node, err := NewNode(Config{
		ID:            "n1",
		Peers:         map[string]string{},
		Storage:       store,
		Transport:     transport,
		ElectionTicks: 1,
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	transport.Register(node)
	node.Tick()
	if node.State() != Leader {
		t.Fatalf("single node should elect itself, got %s", node.State())
	}
	if err := node.Put("durable", "yes", "client-a", 1); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := node.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}

	store2, err := OpenBoltStore(path)
	if err != nil {
		t.Fatalf("reopen bolt: %v", err)
	}
	restarted, err := NewNode(Config{
		ID:        "n1",
		Peers:     map[string]string{},
		Storage:   store2,
		Transport: NewMemoryTransport(),
	})
	if err != nil {
		t.Fatalf("restart node: %v", err)
	}
	value, ok, _ := restarted.Get("durable")
	if !ok || value != "yes" {
		t.Fatalf("restarted value=%q ok=%v", value, ok)
	}
}

func TestSnapshotCompactsLogAndRestoresState(t *testing.T) {
	nodes, _ := newTestCluster(t, 3, 2)
	leader := electLeader(t, nodes)

	if err := leader.Put("a", "1", "client-a", 1); err != nil {
		t.Fatalf("put a: %v", err)
	}
	if err := leader.Put("b", "2", "client-a", 2); err != nil {
		t.Fatalf("put b: %v", err)
	}

	leader.mu.Lock()
	snapshotIndex := leader.snapshotIndex
	logLen := len(leader.log)
	leader.mu.Unlock()
	if snapshotIndex < 2 {
		t.Fatalf("snapshot was not created, snapshotIndex=%d", snapshotIndex)
	}
	if logLen != 0 {
		t.Fatalf("snapshot should compact committed log, len=%d", logLen)
	}

	raw := append([]byte(nil), leader.lastSnapshotState...)
	store := NewMemoryStore()
	if err := store.Save(PersistedState{SnapshotIndex: snapshotIndex, SnapshotTerm: leader.snapshotTerm, LastApplied: snapshotIndex, LastSnapshotState: raw}); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	restored, err := NewNode(Config{ID: "restore", Peers: map[string]string{}, Storage: store, Transport: NewMemoryTransport()})
	if err != nil {
		t.Fatalf("restore snapshot: %v", err)
	}
	value, ok, _ := restored.Get("b")
	if !ok || value != "2" {
		t.Fatalf("restored snapshot value=%q ok=%v", value, ok)
	}
}

func TestLeaderFailureTriggersNewElection(t *testing.T) {
	nodes, _ := newTestCluster(t, 3, 100)
	leader := electLeader(t, nodes)
	if err := leader.Stop(); err != nil {
		t.Fatalf("stop leader: %v", err)
	}

	var newLeader *Node
	for i := 0; i < 20; i++ {
		for _, n := range nodes {
			if n.ID() != leader.ID() {
				n.Tick()
			}
		}
		for _, n := range nodes {
			if n.ID() != leader.ID() && n.State() == Leader {
				newLeader = n
				break
			}
		}
		if newLeader != nil {
			break
		}
	}
	if newLeader == nil {
		t.Fatalf("new leader was not elected after %s stopped", leader.ID())
	}
	if err := newLeader.Put("after-failover", "ok", "client-failover", 1); err != nil {
		t.Fatalf("put after failover: %v", err)
	}
	value, ok, _ := newLeader.Get("after-failover")
	if !ok || value != "ok" {
		t.Fatalf("new leader did not serve writes, value=%q ok=%v", value, ok)
	}
}

func TestFollowerRestartCatchesUpCommittedLog(t *testing.T) {
	nodes, transport := newTestCluster(t, 3, 100)
	leader := electLeader(t, nodes)
	follower := firstFollower(nodes)
	store := follower.storage
	peers := follower.peers
	if err := follower.Stop(); err != nil {
		t.Fatalf("stop follower: %v", err)
	}

	if err := leader.Put("restart-follower", "after-stop", "client-restart", 1); err != nil {
		t.Fatalf("put while follower stopped: %v", err)
	}

	restarted, err := NewNode(Config{
		ID:                follower.ID(),
		Peers:             peers,
		Storage:           store,
		Transport:         transport,
		ElectionTicks:     5,
		HeartbeatTicks:    1,
		SnapshotThreshold: 100,
	})
	if err != nil {
		t.Fatalf("restart follower: %v", err)
	}
	transport.Register(restarted)

	for i := 0; i < 5; i++ {
		leader.Tick()
	}
	value, ok, _ := restarted.Get("restart-follower")
	if !ok || value != "after-stop" {
		t.Fatalf("restarted follower value=%q ok=%v", value, ok)
	}
}

func TestLaggingFollowerCatchesUpFromSnapshot(t *testing.T) {
	nodes, transport := newTestCluster(t, 3, 2)
	leader := electLeader(t, nodes)
	lagging := firstFollower(nodes)
	transport.Block(lagging.ID(), true)

	for i := uint64(1); i <= 3; i++ {
		if err := leader.Put("snap-key", string(rune('0'+i)), "client-snapshot", i); err != nil {
			t.Fatalf("put %d while follower blocked: %v", i, err)
		}
	}
	transport.Block(lagging.ID(), false)
	for i := 0; i < 5; i++ {
		leader.Tick()
	}

	value, ok, _ := lagging.Get("snap-key")
	if !ok || value != "3" {
		t.Fatalf("lagging follower did not catch up from snapshot, value=%q ok=%v", value, ok)
	}
	lagging.mu.Lock()
	snapshotIndex := lagging.snapshotIndex
	lagging.mu.Unlock()
	if snapshotIndex == 0 {
		t.Fatalf("lagging follower did not install a snapshot")
	}
}

func TestLeaderRequiresQuorumToCommitWrites(t *testing.T) {
	nodes, transport := newTestCluster(t, 3, 100)
	leader := electLeader(t, nodes)
	for _, n := range nodes {
		if n.ID() != leader.ID() {
			transport.Block(n.ID(), true)
		}
	}

	err := leader.Put("minority", "bad", "client-minority", 1)
	if err != ErrNoQuorum {
		t.Fatalf("minority write error=%v, want %v", err, ErrNoQuorum)
	}
	_, ok, _ := leader.Get("minority")
	if ok {
		t.Fatalf("minority write was applied without quorum")
	}
}

func TestLeaderCanCommitWithOneFollowerPartitioned(t *testing.T) {
	nodes, transport := newTestCluster(t, 3, 100)
	leader := electLeader(t, nodes)
	transport.Block(firstFollower(nodes).ID(), true)

	if err := leader.Put("majority", "ok", "client-majority", 1); err != nil {
		t.Fatalf("majority write failed: %v", err)
	}
	value, ok, _ := leader.Get("majority")
	if !ok || value != "ok" {
		t.Fatalf("majority write value=%q ok=%v", value, ok)
	}
}

func TestLinearizableReadRequiresQuorum(t *testing.T) {
	nodes, transport := newTestCluster(t, 3, 100)
	leader := electLeader(t, nodes)
	if err := leader.Put("read-key", "ok", "client-read", 1); err != nil {
		t.Fatalf("initial put: %v", err)
	}
	for _, n := range nodes {
		if n.ID() != leader.ID() {
			transport.Block(n.ID(), true)
		}
	}

	_, _, err := leader.LinearizableGet("read-key")
	if err != ErrNoQuorum {
		t.Fatalf("linearizable read error=%v, want %v", err, ErrNoQuorum)
	}
}

func TestLinearizableReadSucceedsWithMajority(t *testing.T) {
	nodes, transport := newTestCluster(t, 3, 100)
	leader := electLeader(t, nodes)
	if err := leader.Put("majority-read", "ok", "client-read", 1); err != nil {
		t.Fatalf("initial put: %v", err)
	}
	transport.Block(firstFollower(nodes).ID(), true)

	value, ok, err := leader.LinearizableGet("majority-read")
	if err != nil {
		t.Fatalf("linearizable read with majority: %v", err)
	}
	if !ok || value != "ok" {
		t.Fatalf("linearizable read value=%q ok=%v", value, ok)
	}
}

func newTestCluster(t *testing.T, size int, snapshotThreshold uint64) ([]*Node, *MemoryTransport) {
	t.Helper()
	transport := NewMemoryTransport()
	nodes := make([]*Node, 0, size)
	for i := 1; i <= size; i++ {
		id := "n" + string(rune('0'+i))
		peers := map[string]string{}
		for j := 1; j <= size; j++ {
			if j == i {
				continue
			}
			peers["n"+string(rune('0'+j))] = ""
		}
		n, err := NewNode(Config{
			ID:                id,
			Peers:             peers,
			Storage:           NewMemoryStore(),
			Transport:         transport,
			ElectionTicks:     i,
			HeartbeatTicks:    1,
			SnapshotThreshold: snapshotThreshold,
		})
		if err != nil {
			t.Fatalf("new node %s: %v", id, err)
		}
		nodes = append(nodes, n)
		transport.Register(n)
	}
	return nodes, transport
}

func electLeader(t *testing.T, nodes []*Node) *Node {
	t.Helper()
	for i := 0; i < 10; i++ {
		for _, n := range nodes {
			n.Tick()
		}
		for _, n := range nodes {
			if n.State() == Leader {
				return n
			}
		}
	}
	t.Fatalf("leader not elected")
	return nil
}

func firstFollower(nodes []*Node) *Node {
	for _, n := range nodes {
		if n.State() != Leader {
			return n
		}
	}
	return nodes[0]
}
