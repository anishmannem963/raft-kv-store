package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestStatusesConvergedRequiresExactlyOneLeader(t *testing.T) {
	base := []status{
		{ID: "node1", State: "follower", CommitIndex: 4, KeyCount: 5, StateHash: "same"},
		{ID: "node2", State: "follower", CommitIndex: 4, KeyCount: 5, StateHash: "same"},
		{ID: "node3", State: "follower", CommitIndex: 4, KeyCount: 5, StateHash: "same"},
	}
	if statusesConverged(base, 5) {
		t.Fatal("leaderless replicas must not pass benchmark preflight")
	}
	base[1].State = "leader"
	if !statusesConverged(base, 5) {
		t.Fatal("one leader with identical replica state should converge")
	}
	base[2].State = "leader"
	if statusesConverged(base, 5) {
		t.Fatal("multiple leaders must not be reported as converged")
	}
}

func TestStatusesConvergedRequiresMatchingStateAndMinimumKeys(t *testing.T) {
	statuses := []status{
		{ID: "node1", State: "leader", CommitIndex: 4, KeyCount: 5, StateHash: "same"},
		{ID: "node2", State: "follower", CommitIndex: 4, KeyCount: 5, StateHash: "same"},
		{ID: "node3", State: "follower", CommitIndex: 4, KeyCount: 5, StateHash: "same"},
	}
	if statusesConverged(statuses, 6) {
		t.Fatal("insufficient key count must not pass")
	}
	statuses[2].StateHash = "different"
	if statusesConverged(statuses, 5) {
		t.Fatal("different state hashes must not pass")
	}
}

func TestWriteRetriesUntilSuccess(t *testing.T) {
	var attempts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) > 20 {
			w.WriteHeader(http.StatusCreated)
			return
		}
		http.Error(w, "retry", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := write(ctx, server.Client(), []string{server.URL}, "test-run", 0); err != nil {
		t.Fatalf("write should retry until success: %v", err)
	}
	if attempts.Load() != 21 {
		t.Fatalf("attempts = %d, want 21", attempts.Load())
	}
}
