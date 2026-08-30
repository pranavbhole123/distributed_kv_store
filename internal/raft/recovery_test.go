package raft

import (
	"path/filepath"
	"testing"
)

type recordingApplier struct {
	entries []LogEntry
}

func (a *recordingApplier) Apply(entry LogEntry) error {
	a.entries = append(a.entries, entry)
	return nil
}

func TestRestartRetainsUncommittedEntryWithoutApplyingIt(t *testing.T) {
	dir := t.TempDir()
	stable := NewFileStableStore(filepath.Join(dir, "raft-state.json"))
	logStore, err := NewFileLogStore(filepath.Join(dir, "raft-log.wal"))
	if err != nil {
		t.Fatal(err)
	}
	entry := LogEntry{Index: 1, Term: 1, Operation: SetOperation, Key: "uncommitted", Value: "value"}
	if err := logStore.Append([]LogEntry{entry}); err != nil {
		t.Fatal(err)
	}
	if err := logStore.Close(); err != nil {
		t.Fatal(err)
	}

	afterRestartLog, err := NewFileLogStore(filepath.Join(dir, "raft-log.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = afterRestartLog.Close() })
	applier := &recordingApplier{}
	r, err := NewWithLog(testConfig(1, 3), stable, afterRestartLog, applier, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.log) != 1 || r.log[0] != entry {
		t.Fatalf("recovered log = %+v, want uncommitted entry", r.log)
	}
	if len(applier.entries) != 0 || r.CommitIndex() != 0 || r.LastApplied() != 0 {
		t.Fatalf("uncommitted entry was applied: entries=%+v commit=%d applied=%d", applier.entries, r.CommitIndex(), r.LastApplied())
	}
}

func TestRestartReplaysOnlyCommittedPrefix(t *testing.T) {
	dir := t.TempDir()
	stable := NewFileStableStore(filepath.Join(dir, "raft-state.json"))
	if err := stable.Save(StableState{CurrentTerm: 4, VotedFor: 2, CommitIndex: 1}); err != nil {
		t.Fatal(err)
	}
	logStore, err := NewFileLogStore(filepath.Join(dir, "raft-log.wal"))
	if err != nil {
		t.Fatal(err)
	}
	committed := LogEntry{Index: 1, Term: 4, Operation: SetOperation, Key: "committed", Value: "yes"}
	uncommitted := LogEntry{Index: 2, Term: 4, Operation: SetOperation, Key: "uncommitted", Value: "no"}
	if err := logStore.Append([]LogEntry{committed, uncommitted}); err != nil {
		t.Fatal(err)
	}
	if err := logStore.Close(); err != nil {
		t.Fatal(err)
	}

	afterRestartLog, err := NewFileLogStore(filepath.Join(dir, "raft-log.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = afterRestartLog.Close() })
	applier := &recordingApplier{}
	r, err := NewWithLog(testConfig(1, 3), stable, afterRestartLog, applier, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(applier.entries) != 1 || applier.entries[0] != committed {
		t.Fatalf("replayed entries = %+v, want only %+v", applier.entries, committed)
	}
	if r.CurrentTerm() != 4 || r.CommitIndex() != 1 || r.LastApplied() != 1 {
		t.Fatalf("recovered term=%d commit=%d applied=%d, want 4/1/1", r.CurrentTerm(), r.CommitIndex(), r.LastApplied())
	}
	persisted, err := stable.Load()
	if err != nil {
		t.Fatal(err)
	}
	if persisted.LastApplied != 1 {
		t.Fatalf("persisted last_applied = %d, want 1", persisted.LastApplied)
	}
}
