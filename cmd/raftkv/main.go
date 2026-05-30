package main

import (
	"encoding/json"
	"flag"
	"html/template"
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
		mux.HandleFunc("/debug/status", statusHandler(node))
		mux.HandleFunc("/debug/ui", debugUIHandler(node))
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

func statusHandler(node *raft.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		if err := json.NewEncoder(w).Encode(node.Status()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func debugUIHandler(node *raft.Node) http.HandlerFunc {
	tpl := template.Must(template.New("debug-ui").Parse(debugHTML))
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		if err := tpl.Execute(w, node.Status()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

const debugHTML = `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Mini-RaftKV Debug</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f7f9;
      --panel: #ffffff;
      --ink: #1f2937;
      --muted: #6b7280;
      --line: #d9dee7;
      --green: #15803d;
      --blue: #1d4ed8;
      --amber: #b45309;
      --red: #b91c1c;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    main {
      width: min(1120px, calc(100vw - 32px));
      margin: 32px auto;
    }
    header {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 20px;
    }
    h1 {
      margin: 0;
      font-size: 28px;
      line-height: 1.2;
    }
    .sub {
      margin-top: 6px;
      color: var(--muted);
      font-size: 14px;
    }
    .badge {
      display: inline-flex;
      align-items: center;
      height: 32px;
      padding: 0 12px;
      border-radius: 999px;
      background: #e8eefc;
      color: var(--blue);
      font-weight: 700;
      white-space: nowrap;
    }
    .badge.leader { background: #dcfce7; color: var(--green); }
    .badge.candidate { background: #fef3c7; color: var(--amber); }
    .badge.stopped { background: #fee2e2; color: var(--red); }
    .grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 16px;
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 16px;
      box-shadow: 0 1px 2px rgba(15, 23, 42, .04);
    }
    .label {
      color: var(--muted);
      font-size: 13px;
      margin-bottom: 6px;
    }
    .value {
      font-size: 24px;
      font-weight: 750;
      line-height: 1.2;
      overflow-wrap: anywhere;
    }
    .wide {
      display: grid;
      grid-template-columns: 1.15fr .85fr;
      gap: 12px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }
    th, td {
      text-align: left;
      padding: 10px 0;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
    }
    th { color: var(--muted); font-weight: 650; }
    tr:last-child td { border-bottom: 0; }
    code {
      background: #eef2f7;
      border-radius: 6px;
      padding: 2px 6px;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 13px;
    }
    .json {
      margin: 0;
      padding: 14px;
      background: #111827;
      color: #d1fae5;
      border-radius: 8px;
      overflow: auto;
      max-height: 420px;
      font-size: 12px;
      line-height: 1.5;
    }
    @media (max-width: 860px) {
      .grid, .wide { grid-template-columns: 1fr; }
      header { display: block; }
      .badge { margin-top: 12px; }
    }
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <h1>Mini-RaftKV Node {{.ID}}</h1>
        <div class="sub">自动刷新节点状态；JSON 接口在 <code>/debug/status</code>，Prometheus 指标在 <code>/metrics</code>。</div>
      </div>
      <div id="roleBadge" class="badge">{{.State}}</div>
    </header>

    <section class="grid">
      <div class="card"><div class="label">Leader</div><div class="value" id="leaderId">{{.LeaderID}}</div></div>
      <div class="card"><div class="label">Term</div><div class="value" id="currentTerm">{{.CurrentTerm}}</div></div>
      <div class="card"><div class="label">Commit Index</div><div class="value" id="commitIndex">{{.CommitIndex}}</div></div>
      <div class="card"><div class="label">Last Applied</div><div class="value" id="lastApplied">{{.LastApplied}}</div></div>
    </section>

    <section class="wide">
      <div class="card">
        <div class="label">Raft Progress</div>
        <table>
          <tbody id="progressRows"></tbody>
        </table>
      </div>
      <div class="card">
        <div class="label">Peers</div>
        <table>
          <thead><tr><th>Node</th><th>Address</th><th>next / match</th></tr></thead>
          <tbody id="peerRows"></tbody>
        </table>
      </div>
    </section>

    <section class="card" style="margin-top: 12px;">
      <div class="label">Raw Status</div>
      <pre class="json" id="rawJson"></pre>
    </section>
  </main>
  <script>
    function roleClass(state, stopped) {
      if (stopped) return "badge stopped";
      if (state === "Leader") return "badge leader";
      if (state === "Candidate") return "badge candidate";
      return "badge";
    }
    function setText(id, value) {
      document.getElementById(id).textContent = value || "-";
    }
    function renderProgress(data) {
      const rows = [
        ["Last Log", data.lastLogIndex + " @ term " + data.lastLogTerm],
        ["Snapshot", data.snapshotIndex + " @ term " + data.snapshotTerm],
        ["Log Length", data.logLength],
        ["Election", data.electionElapsed + " / " + data.electionTicks + " ticks"],
        ["Heartbeat", data.heartbeatTicks + " ticks"],
        ["Snapshot Threshold", data.snapshotThreshold],
        ["Voted For", data.votedFor || "-"]
      ];
      document.getElementById("progressRows").innerHTML = rows.map(([k, v]) => "<tr><th>" + k + "</th><td>" + v + "</td></tr>").join("");
    }
    function renderPeers(data) {
      const peers = data.peers || {};
      const next = data.nextIndex || {};
      const match = data.matchIndex || {};
      const rows = Object.keys(peers).sort().map(id => {
        return "<tr><td><code>" + id + "</code></td><td>" + peers[id] + "</td><td>" + (next[id] || "-") + " / " + (match[id] || "-") + "</td></tr>";
      });
      document.getElementById("peerRows").innerHTML = rows.length ? rows.join("") : "<tr><td colspan='3'>single node</td></tr>";
    }
    async function refresh() {
      const res = await fetch("/debug/status", { cache: "no-store" });
      const data = await res.json();
      document.querySelector("h1").textContent = "Mini-RaftKV Node " + data.id;
      const badge = document.getElementById("roleBadge");
      badge.textContent = data.stopped ? "Stopped" : data.state;
      badge.className = roleClass(data.state, data.stopped);
      setText("leaderId", data.leaderId);
      setText("currentTerm", data.currentTerm);
      setText("commitIndex", data.commitIndex);
      setText("lastApplied", data.lastApplied);
      renderProgress(data);
      renderPeers(data);
      document.getElementById("rawJson").textContent = JSON.stringify(data, null, 2);
    }
    refresh();
    setInterval(refresh, 1500);
  </script>
</body>
</html>`
