package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

var ErrNoState = errors.New("raft state does not exist")

type PersistentState struct {
	CurrentTerm int        `json:"current_term"`
	VotedFor    string     `json:"voted_for,omitempty"`
	Log         []LogEntry `json:"log"`
	CommitIndex int        `json:"commit_index"`
}

type Storage interface {
	Load() (PersistentState, error)
	Save(PersistentState) error
}

type MemoryStorage struct {
	mu    sync.Mutex
	state *PersistentState
}

func (s *MemoryStorage) Load() (PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == nil {
		return PersistentState{}, ErrNoState
	}
	return clonePersistentState(*s.state), nil
}

func (s *MemoryStorage) Save(state PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copy := clonePersistentState(state)
	s.state = &copy
	return nil
}

type FileStorage struct {
	mu   sync.Mutex
	path string
}

func NewFileStorage(dataDir string) *FileStorage {
	return &FileStorage{path: filepath.Join(dataDir, "raft-state.json")}
}

func (s *FileStorage) Load() (PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	contents, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return PersistentState{}, ErrNoState
	}
	if err != nil {
		return PersistentState{}, fmt.Errorf("read raft state: %w", err)
	}

	var state PersistentState
	if err := json.Unmarshal(contents, &state); err != nil {
		return PersistentState{}, fmt.Errorf("decode raft state: %w", err)
	}
	state.Log = append([]LogEntry(nil), state.Log...)
	return state, nil
}

func (s *FileStorage) Save(state PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(s.path), 0o750); err != nil {
		return fmt.Errorf("create data directory: %w", err)
	}
	contents, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode raft state: %w", err)
	}

	temporary, err := os.CreateTemp(filepath.Dir(s.path), ".raft-state-*")
	if err != nil {
		return fmt.Errorf("create temporary state: %w", err)
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)

	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return fmt.Errorf("set state permissions: %w", err)
	}
	if _, err := temporary.Write(contents); err != nil {
		temporary.Close()
		return fmt.Errorf("write raft state: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return fmt.Errorf("sync raft state: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close raft state: %w", err)
	}
	if err := os.Rename(temporaryName, s.path); err != nil {
		return fmt.Errorf("replace raft state: %w", err)
	}
	return syncDirectory(filepath.Dir(s.path))
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open data directory: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync data directory: %w", err)
	}
	return nil
}

func clonePersistentState(state PersistentState) PersistentState {
	state.Log = append([]LogEntry(nil), state.Log...)
	return state
}
