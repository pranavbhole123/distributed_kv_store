package raft

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/pranavbhole123/distributed_kv_store/internal/snapshot"
	"github.com/pranavbhole123/distributed_kv_store/internal/store"
)

type recordingApplier struct {
	entries []LogEntry
}

type memoryStoreApplier struct {
	store *store.MemoryStore
}

func (a memoryStoreApplier) Apply(entry LogEntry) error {
	switch entry.Operation {
	case NoopOperation:
		return nil
	case SetOperation:
		return a.store.Set(entry.Key, entry.Value)
	case DeleteOperation:
		return a.store.Delete(entry.Key)
	default:
		return fmt.Errorf("unknown operation %q", entry.Operation)
	}
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

func TestRestartRestoresSnapshotThenReplaysCommittedSuffix(t *testing.T) {
	dir := t.TempDir()

	beforeSnapshot := store.NewMemoryStore(20)
	if err := beforeSnapshot.Set("from-snapshot", "kept"); err != nil {
		t.Fatal(err)
	}
	if err := beforeSnapshot.Set("deleted-by-log", "old"); err != nil {
		t.Fatal(err)
	}
	snapshotData, err := beforeSnapshot.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	snapshotStore := snapshot.NewFileStore(filepath.Join(dir, "raft-snapshot.json"))
	if err := snapshotStore.Save(snapshot.Snapshot{
		LastIncludedIndex: 500,
		LastIncludedTerm:  7,
		Data:              snapshotData,
	}); err != nil {
		t.Fatal(err)
	}

	stable := NewFileStableStore(filepath.Join(dir, "raft-state.json"))
	if err := stable.Save(StableState{CurrentTerm: 8, VotedFor: NoVote, CommitIndex: 502, LastApplied: 500}); err != nil {
		t.Fatal(err)
	}
	logStore, err := NewFileLogStore(filepath.Join(dir, "raft-log.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = logStore.Close() })
	if err := logStore.Append([]LogEntry{
		{Index: 501, Term: 7, Operation: SetOperation, Key: "from-log", Value: "applied"},
		{Index: 502, Term: 8, Operation: DeleteOperation, Key: "deleted-by-log"},
		{Index: 503, Term: 8, Operation: SetOperation, Key: "uncommitted", Value: "hidden"},
	}); err != nil {
		t.Fatal(err)
	}

	recoveredStore := store.NewMemoryStore(20)
	r, err := NewWithSnapshot(
		testConfig(1, 3),
		stable,
		logStore,
		memoryStoreApplier{store: recoveredStore},
		recoveredStore,
		snapshotStore,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if metadata := r.SnapshotMetadata(); metadata.LastIncludedIndex != 500 || metadata.LastIncludedTerm != 7 {
		t.Fatalf("snapshot metadata = %+v, want index/term 500/7", metadata)
	}
	if r.CommitIndex() != 502 || r.LastApplied() != 502 {
		t.Fatalf("recovered commit/applied = %d/%d, want 502/502", r.CommitIndex(), r.LastApplied())
	}
	if value, err := recoveredStore.Get("from-snapshot"); err != nil || value != "kept" {
		t.Fatalf("snapshot value = %q, %v; want kept, nil", value, err)
	}
	if value, err := recoveredStore.Get("from-log"); err != nil || value != "applied" {
		t.Fatalf("committed suffix value = %q, %v; want applied, nil", value, err)
	}
	if _, err := recoveredStore.Get("deleted-by-log"); err == nil {
		t.Fatal("committed DELETE suffix entry was not replayed")
	}
	if _, err := recoveredStore.Get("uncommitted"); err == nil {
		t.Fatal("uncommitted suffix entry became visible during recovery")
	}
	persisted, err := stable.Load()
	if err != nil {
		t.Fatal(err)
	}
	if persisted.LastApplied != 502 {
		t.Fatalf("persisted last applied = %d, want 502", persisted.LastApplied)
	}
}
