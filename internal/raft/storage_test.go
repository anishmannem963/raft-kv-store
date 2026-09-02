package raft

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFileStorageRoundTrip(t *testing.T) {
	storage := NewFileStorage(t.TempDir())
	want := PersistentState{
		CurrentTerm: 7,
		VotedFor:    "node2",
		Log: []LogEntry{{
			Term: 6,
			Command: Command{
				Operation: "put",
				Key:       "mission",
				Value:     "recovery",
			},
		}},
		CommitIndex: 0,
	}
	if err := storage.Save(want); err != nil {
		t.Fatal(err)
	}

	got, err := storage.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.CurrentTerm != want.CurrentTerm || got.VotedFor != want.VotedFor || got.CommitIndex != 0 {
		t.Fatalf("unexpected restored state: %+v", got)
	}
	if len(got.Log) != 1 || got.Log[0].Command.Value != "recovery" {
		t.Fatalf("unexpected restored log: %+v", got.Log)
	}
}

func TestNodeRestoresCommittedState(t *testing.T) {
	storage := NewFileStorage(t.TempDir())
	state := PersistentState{
		CurrentTerm: 4,
		VotedFor:    "node1",
		Log: []LogEntry{{
			Term: 3,
			Command: Command{
				Operation: "put",
				Key:       "durable",
				Value:     "yes",
			},
		}},
		CommitIndex: 0,
	}
	if err := storage.Save(state); err != nil {
		t.Fatal(err)
	}

	node, err := NewNodeWithStorage("node1", map[string]string{"node1": "one"}, fakeTransport{}, storage)
	if err != nil {
		t.Fatal(err)
	}
	value, ok := node.Get("durable")
	if !ok || value != "yes" {
		t.Fatalf("committed value was not restored: value=%q ok=%v", value, ok)
	}
	status := node.Status()
	if status.Term != 4 || status.CommitIndex != 0 || !status.StorageOK {
		t.Fatalf("unexpected restored status: %+v", status)
	}
}

func TestFileStorageRejectsCorruptState(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "raft-state.json"), []byte("not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := NewNodeWithStorage("node1", map[string]string{"node1": "one"}, fakeTransport{}, NewFileStorage(directory))
	if err == nil {
		t.Fatal("expected corrupt persisted state to fail startup")
	}
}
