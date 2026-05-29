package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/juanjuandog/mini-raftkv/internal/pb"
	"github.com/juanjuandog/mini-raftkv/internal/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestGRPCClusterForwardsFollowerWritesToLeader(t *testing.T) {
	cluster := startGRPCCluster(t, 3)
	leader := waitForLeader(t, cluster.nodes)
	for i := 0; i < 3; i++ {
		for _, n := range cluster.nodes {
			n.Tick()
		}
	}

	followerAddr := ""
	for _, n := range cluster.nodes {
		if n.ID() != leader.ID() {
			followerAddr = cluster.addrs[n.ID()]
			break
		}
	}
	if followerAddr == "" {
		t.Fatalf("no follower address found")
	}

	client, closeFn := dialTestClient(t, followerAddr)
	defer closeFn()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	putResp, err := client.Put(ctx, &pb.PutRequest{
		Key: "grpc-key", Value: "grpc-value", ClientId: "grpc-test", RequestId: 1,
	})
	if err != nil {
		t.Fatalf("put via follower: %v", err)
	}
	if !putResp.Ok || putResp.LeaderId != leader.ID() {
		t.Fatalf("put response ok=%v leader=%q want %q error=%q", putResp.Ok, putResp.LeaderId, leader.ID(), putResp.Error)
	}

	for _, addr := range cluster.addrs {
		readClient, readClose := dialTestClient(t, addr)
		getResp, err := readClient.Get(ctx, &pb.GetRequest{Key: "grpc-key"})
		readClose()
		if err != nil {
			t.Fatalf("get %s: %v", addr, err)
		}
		if !getResp.Found || getResp.Value != "grpc-value" {
			t.Fatalf("get %s found=%v value=%q", addr, getResp.Found, getResp.Value)
		}
	}
}

func TestGRPCClusterForwardsFollowerReadsToLeader(t *testing.T) {
	cluster := startGRPCCluster(t, 3)
	leader := waitForLeader(t, cluster.nodes)
	for i := 0; i < 3; i++ {
		for _, n := range cluster.nodes {
			n.Tick()
		}
	}

	leaderClient, leaderClose := dialTestClient(t, cluster.addrs[leader.ID()])
	defer leaderClose()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := leaderClient.Put(ctx, &pb.PutRequest{
		Key: "linear-read", Value: "leader-value", ClientId: "grpc-test", RequestId: 2,
	}); err != nil {
		t.Fatalf("put via leader: %v", err)
	}

	for _, n := range cluster.nodes {
		if n.ID() == leader.ID() {
			continue
		}
		client, closeFn := dialTestClient(t, cluster.addrs[n.ID()])
		resp, err := client.Get(ctx, &pb.GetRequest{Key: "linear-read"})
		closeFn()
		if err != nil {
			t.Fatalf("get via follower %s: %v", n.ID(), err)
		}
		if !resp.Found || resp.Value != "leader-value" || resp.LeaderId != leader.ID() {
			t.Fatalf("follower read found=%v value=%q leader=%q", resp.Found, resp.Value, resp.LeaderId)
		}
	}
}

type grpcTestCluster struct {
	nodes   []*raft.Node
	addrs   map[string]string
	servers []*grpc.Server
}

func startGRPCCluster(t *testing.T, size int) grpcTestCluster {
	t.Helper()
	addrs := map[string]string{}
	listeners := map[string]net.Listener{}
	for i := 1; i <= size; i++ {
		id := nodeID(i)
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen %s: %v", id, err)
		}
		addrs[id] = lis.Addr().String()
		listeners[id] = lis
	}

	nodes := make([]*raft.Node, 0, size)
	servers := make([]*grpc.Server, 0, size)
	for i := 1; i <= size; i++ {
		id := nodeID(i)
		peers := map[string]string{}
		for peerID, addr := range addrs {
			if peerID != id {
				peers[peerID] = addr
			}
		}
		node, err := raft.NewNode(raft.Config{
			ID: id, Peers: peers, Storage: raft.NewMemoryStore(),
			Transport:     serverTransportForTest(peers),
			ElectionTicks: i, HeartbeatTicks: 1, SnapshotThreshold: 100,
		})
		if err != nil {
			t.Fatalf("new node %s: %v", id, err)
		}
		nodes = append(nodes, node)
		grpcServer := grpc.NewServer()
		pb.RegisterRaftKVServer(grpcServer, NewWithPeers(node, peers))
		servers = append(servers, grpcServer)
		go func(lis net.Listener, srv *grpc.Server) {
			_ = srv.Serve(lis)
		}(listeners[id], grpcServer)
	}
	t.Cleanup(func() {
		for _, srv := range servers {
			srv.Stop()
		}
	})
	return grpcTestCluster{nodes: nodes, addrs: addrs, servers: servers}
}

func serverTransportForTest(peers map[string]string) *GRPCTransport {
	return NewGRPCTransport(peers)
}

func waitForLeader(t *testing.T, nodes []*raft.Node) *raft.Node {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			n.Tick()
		}
		for _, n := range nodes {
			if n.State() == raft.Leader {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("leader not elected")
	return nil
}

func dialTestClient(t *testing.T, addr string) (pb.RaftKVClient, func()) {
	t.Helper()
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	return pb.NewRaftKVClient(conn), func() { _ = conn.Close() }
}

func nodeID(i int) string {
	return string(rune('n')) + string(rune('0'+i))
}
