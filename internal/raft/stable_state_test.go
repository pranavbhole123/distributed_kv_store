package raft

import (
	"path/filepath"
	"testing"
)

func TestFileStableStoreReturnsNoVoteWhenStateDoesNotExist(t *testing.T) {
	store := NewFileStableStore(filepath.Join(t.TempDir(), "raft-state.json"))

	state, err := store.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if state != (StableState{VotedFor: NoVote}) {
		t.Fatalf("Load() = %+v, want term 0 and no vote", state)
	}
}

func TestFileStableStorePersistsTermAndVote(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state.json")
	store := NewFileStableStore(path)
	want := StableState{CurrentTerm: 5, VotedFor: 1}

	if err := store.Save(want); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	// A new store represents a process restart.
	afterRestart := NewFileStableStore(path)
	got, err := afterRestart.Load()
	if err != nil {
		t.Fatalf("Load() after restart error = %v", err)
	}
	if got != want {
		t.Fatalf("Load() after restart = %+v, want %+v", got, want)
	}
}
