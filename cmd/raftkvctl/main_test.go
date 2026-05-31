package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFetchStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/debug/status" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"id":"n1","state":"Leader","leaderId":"n1","currentTerm":3,"commitIndex":9,"lastApplied":9,"snapshotIndex":4}`))
	}))
	defer srv.Close()

	status, err := fetchStatus(srv.Client(), strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatalf("fetch status: %v", err)
	}
	if status.ID != "n1" || status.State != "Leader" || status.CurrentTerm != 3 || status.SnapshotIndex != 4 {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestSplitAddrs(t *testing.T) {
	got := splitAddrs(" 127.0.0.1:9001, ,127.0.0.1:9002 ")
	want := []string{"127.0.0.1:9001", "127.0.0.1:9002"}
	if len(got) != len(want) {
		t.Fatalf("len=%d want=%d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got[%d]=%q want=%q", i, got[i], want[i])
		}
	}
}

func TestPrintStatuses(t *testing.T) {
	var out bytes.Buffer
	printStatuses(&out, []debugStatus{
		{Addr: "127.0.0.1:9001", ID: "n1", State: "Leader", LeaderID: "n1", CurrentTerm: 2, CommitIndex: 7, LastApplied: 7},
		{Addr: "127.0.0.1:9002", Error: "connection refused"},
	})

	body := out.String()
	for _, want := range []string{"ADDR", "127.0.0.1:9001", "Leader", "connection refused"} {
		if !strings.Contains(body, want) {
			t.Fatalf("output missing %q:\n%s", want, body)
		}
	}
}
