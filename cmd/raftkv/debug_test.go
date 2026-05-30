package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/juanjuandog/mini-raftkv/internal/raft"
)

func TestStatusHandlerReturnsNodeStatus(t *testing.T) {
	node, err := raft.NewNode(raft.Config{
		ID:        "n1",
		Peers:     map[string]string{"n2": "127.0.0.1:7002"},
		Storage:   raft.NewMemoryStore(),
		Transport: raft.NewMemoryTransport(),
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/debug/status", nil)
	rec := httptest.NewRecorder()
	statusHandler(node).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status code=%d", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); !strings.Contains(got, "application/json") {
		t.Fatalf("content-type=%q", got)
	}
	var status raft.NodeStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &status); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if status.ID != "n1" || status.State != raft.Follower || status.Peers["n2"] == "" {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestDebugUIHandlerReturnsDashboard(t *testing.T) {
	node, err := raft.NewNode(raft.Config{
		ID:        "n1",
		Peers:     map[string]string{},
		Storage:   raft.NewMemoryStore(),
		Transport: raft.NewMemoryTransport(),
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/debug/ui", nil)
	rec := httptest.NewRecorder()
	debugUIHandler(node).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status code=%d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "Mini-RaftKV Node n1") || !strings.Contains(body, "/debug/status") {
		t.Fatalf("dashboard body missing expected content: %s", body)
	}
}
