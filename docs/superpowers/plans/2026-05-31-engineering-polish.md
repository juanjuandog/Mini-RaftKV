# Engineering Polish Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Improve Mini-RaftKV's GitHub professionalism, command-line observability, failover demo, and consistency-test evidence.

**Architecture:** Keep all improvements inside the existing Go project. The node already exposes `/debug/status`; `raftkvctl` will consume that endpoint for status and cluster views, scripts will compose existing binaries, and tests will extend the existing in-memory Raft test style.

**Tech Stack:** Go, gRPC, HTTP JSON, shell scripts, bbolt, GitHub Actions.

---

### Task 1: Repository Hygiene

**Files:**
- Modify: `.gitignore`
- Untrack generated runtime artifacts: `bin/`, `data/*.db`, `logs/*.log`, `raftkv`, `raftkvctl`

- [ ] Add ignore rules for build outputs, local databases, logs, and temporary files.
- [ ] Remove already tracked runtime artifacts from Git while keeping local files available.
- [ ] Verify `git status --short` only shows intentional source/doc changes.

### Task 2: CLI Observability

**Files:**
- Modify: `cmd/raftkvctl/main.go`
- Test: `cmd/raftkvctl/main_test.go`

- [ ] Add `status`, `cluster`, and `watch` commands backed by `/debug/status`.
- [ ] Parse comma-separated HTTP addresses for cluster views.
- [ ] Render compact tabular output showing node, state, leader, term, commit index, applied index, and snapshot index.
- [ ] Unit test JSON decoding, cluster collection, and table rendering.

### Task 3: Failover Demo

**Files:**
- Create: `scripts/failover-demo.sh`
- Modify: `README.md`

- [ ] Start a local three-node cluster using existing configs.
- [ ] Discover the current leader from `/debug/status`.
- [ ] Write data, stop the leader, wait for a new leader, write again, and verify reads still work.
- [ ] Document how to run the demo and which Debug UI URLs to open.

### Task 4: Strong-Consistency Evidence

**Files:**
- Modify: `internal/raft/cluster_test.go`
- Modify: `README.md`

- [ ] Add a test proving a leader without quorum rejects linearizable reads.
- [ ] Add a test proving a leader without quorum rejects writes.
- [ ] Explain in README that reads are served by the leader only after quorum confirmation.

### Task 5: Verification and Delivery

**Files:**
- No new files

- [ ] Run `gofmt`.
- [ ] Run `go test ./...`.
- [ ] Run `go build ./cmd/raftkv` and `go build ./cmd/raftkvctl`.
- [ ] Commit and push to GitHub.
