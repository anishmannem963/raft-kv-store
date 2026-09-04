package main

import "testing"

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
