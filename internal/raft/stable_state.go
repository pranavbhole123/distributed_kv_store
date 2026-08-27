// Package raft contains the Raft consensus state machine.
package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const NoVote = -1

// StableState is the small part of Raft state that must survive a crash.
// The log belongs to Phase 4; it is deliberately not represented here yet.
type StableState struct {
	CurrentTerm uint64 `json:"current_term"`
	VotedFor    int    `json:"voted_for"`
}

// StableStore makes the Raft state machine independent of its persistence format.
type StableStore interface {
	Load() (StableState, error)
	Save(StableState) error
}

// FileStableStore stores one atomically-replaced JSON document per node.
type FileStableStore struct {
	path string
	mu   sync.Mutex
}

func NewFileStableStore(path string) *FileStableStore {
	return &FileStableStore{path: path}
}

func (s *FileStableStore) Load() (StableState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	file, err := os.Open(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return StableState{VotedFor: NoVote}, nil
	}
	if err != nil {
		return StableState{}, fmt.Errorf("open Raft state %q: %w", s.path, err)
	}
	defer file.Close()

	var state StableState
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&state); err != nil {
		return StableState{}, fmt.Errorf("decode Raft state %q: %w", s.path, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return StableState{}, fmt.Errorf("decode Raft state %q: contains multiple JSON values", s.path)
		}
		return StableState{}, fmt.Errorf("decode Raft state %q: %w", s.path, err)
	}
	if state.VotedFor < NoVote {
		return StableState{}, fmt.Errorf("decode Raft state %q: invalid voted_for %d", s.path, state.VotedFor)
	}
	return state, nil
}

func (s *FileStableStore) Save(state StableState) error {
	if state.VotedFor < NoVote {
		return fmt.Errorf("invalid voted_for %d", state.VotedFor)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create Raft state directory %q: %w", dir, err)
	}

	temporary, err := os.CreateTemp(dir, ".raft-state-*")
	if err != nil {
		return fmt.Errorf("create temporary Raft state: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)

	encoder := json.NewEncoder(temporary)
	if err := encoder.Encode(state); err != nil {
		temporary.Close()
		return fmt.Errorf("encode Raft state: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return fmt.Errorf("sync temporary Raft state: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary Raft state: %w", err)
	}
	if err := os.Rename(temporaryPath, s.path); err != nil {
		return fmt.Errorf("replace Raft state %q: %w", s.path, err)
	}

	directory, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open Raft state directory %q: %w", dir, err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync Raft state directory %q: %w", dir, err)
	}
	return nil
}
