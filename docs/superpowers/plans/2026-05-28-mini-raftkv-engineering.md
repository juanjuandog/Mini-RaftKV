# Mini-RaftKV Engineering Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a complete Go Mini-RaftKV project with Raft consensus, replicated KV operations, persistence, snapshots, gRPC APIs, and Prometheus metrics.

**Architecture:** The project has a Raft core package that is transport-agnostic, a storage package backed by bbolt, a manually registered gRPC API package matching the checked-in Protobuf contract, and a CLI binary for running nodes. Tests use deterministic in-memory networking for fast consensus scenarios while the production entrypoint uses gRPC.

**Tech Stack:** Go 1.26, gRPC, Protobuf contract, bbolt, Prometheus client, standard library tests.

---

### File Structure

- `go.mod`: module and dependencies.
- `api/raftkv.proto`: Protobuf service and message contract.
- `internal/pb/messages.go`: Go message structs used by service bindings.
- `internal/pb/grpc.go`: Hand-written gRPC client/server registration matching the proto service.
- `internal/raft/types.go`: Raft states, log entries, commands, request/response structs.
- `internal/raft/storage.go`: Persistence interfaces and bbolt implementation.
- `internal/raft/state_machine.go`: KV apply logic, dedupe cache, snapshots.
- `internal/raft/node.go`: Election, heartbeats, log replication, commit/apply loop.
- `internal/raft/memory_transport.go`: Deterministic test transport.
- `internal/raft/metrics.go`: Prometheus collectors.
- `internal/server/server.go`: gRPC API adapter for Raft nodes.
- `cmd/raftkv/main.go`: CLI binary for node startup.
- `internal/raft/cluster_test.go`: End-to-end RaftKV behavior tests.

### Tasks

- [ ] Create module structure, proto contract, and dependencies.
- [ ] Write tests for leader election and replicated `Put/Get/Delete`.
- [ ] Write tests for idempotent duplicate client requests.
- [ ] Write tests for log conflict repair through `nextIndex` backoff.
- [ ] Write tests for restart recovery from bbolt state.
- [ ] Write tests for snapshot creation and restore.
- [ ] Implement Raft core and state machine until tests pass.
- [ ] Implement gRPC service/client bindings and CLI.
- [ ] Add Prometheus metrics and HTTP metrics endpoint.
- [ ] Run `gofmt ./...` and `go test ./...`.

### Self-Review

- Spec coverage: Raft roles, election, vote, heartbeat, log replication, commit index, KV apply, conflict repair, idempotency, persistence, snapshot, tests, gRPC, Protobuf contract, and Prometheus are covered.
- Placeholder scan: no TBD/TODO placeholders are required for implementation.
- Type consistency: package boundaries and names are fixed above and used consistently.
