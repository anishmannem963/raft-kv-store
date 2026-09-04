package raft

import "testing"

func TestStateHashIsDeterministicAndContentSensitive(t *testing.T) {
	left := map[string]string{"beta": "2", "alpha": "1"}
	right := map[string]string{"alpha": "1", "beta": "2"}
	if stateHash(left) != stateHash(right) {
		t.Fatal("equal state produced different hashes")
	}
	right["beta"] = "changed"
	if stateHash(left) == stateHash(right) {
		t.Fatal("different state produced the same hash")
	}
}
