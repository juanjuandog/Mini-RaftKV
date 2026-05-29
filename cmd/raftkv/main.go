package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/juanjuandog/mini-raftkv/internal/config"
	"github.com/juanjuandog/mini-raftkv/internal/pb"
	"github.com/juanjuandog/mini-raftkv/internal/raft"
	"github.com/juanjuandog/mini-raftkv/internal/server"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
)

func main() {
	configPath := flag.String("config", "", "YAML config path")
	id := flag.String("id", "n1", "node id")
	addr := flag.String("addr", "127.0.0.1:7001", "gRPC listen address")
	metricsAddr := flag.String("metrics", "127.0.0.1:9001", "Prometheus metrics listen address")
	data := flag.String("data", "data/n1.db", "bbolt data path")
	peerList := flag.String("peers", "", "comma-separated peerID=addr list")
	flag.Parse()

	cfg := config.NodeConfig{
		ID: *id, Addr: *addr, MetricsAddr: *metricsAddr, DataPath: *data,
		Peers: config.ParsePeers(*peerList, *id),
	}
	if *configPath != "" {
		loaded, err := config.Load(*configPath)
		if err != nil {
			log.Fatalf("load config: %v", err)
		}
		cfg = loaded
	} else {
		cfg.ApplyDefaults()
	}

	store, err := raft.OpenBoltStore(cfg.DataPath)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	reg := prometheus.NewRegistry()
	node, err := raft.NewNode(raft.Config{
		ID: cfg.ID, Peers: cfg.Peers, Storage: store, Transport: server.NewGRPCTransport(cfg.Peers),
		ElectionTicks: cfg.ElectionTicks, ElectionJitterTicks: cfg.ElectionJitterTicks,
		HeartbeatTicks: cfg.HeartbeatTicks, SnapshotThreshold: cfg.SnapshotThreshold,
		Metrics: raft.NewMetrics(reg, cfg.ID),
	})
	if err != nil {
		log.Fatalf("new raft node: %v", err)
	}

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			node.Tick()
		}
	}()

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
		log.Printf("metrics listening on %s", cfg.MetricsAddr)
		if err := http.ListenAndServe(cfg.MetricsAddr, mux); err != nil {
			log.Fatalf("metrics server: %v", err)
		}
	}()

	grpcServer := grpc.NewServer()
	pb.RegisterRaftKVServer(grpcServer, server.NewWithPeers(node, cfg.Peers))
	listener, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("raftkv node %s listening on %s with peers %v", cfg.ID, cfg.Addr, cfg.Peers)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("grpc serve: %v", err)
	}
}
