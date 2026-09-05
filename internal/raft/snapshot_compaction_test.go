package raft

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pranavbhole123/distributed_kv_store/internal/snapshot"
	"github.com/pranavbhole123/distributed_kv_store/internal/store"
)

type failingSnapshotStore struct {
	err error
}

func (s failingSnapshotStore) Load() (snapshot.Snapshot, bool, error) {
	return snapshot.Snapshot{}, false, nil
}

func (s failingSnapshotStore) Save(snapshot.Snapshot) error {
	return s.err
}

// failingCompactLogStore lets this test stop immediately after the snapshot is
// durable but before the old log prefix is compacted.
type failingCompactLogStore struct {
	LogStore
	err error
}

func (s failingCompactLogStore) CompactThrough(uint64) error { return s.err }

func TestSnapshotSaveFailureLeavesLogAuthoritative(t *testing.T) {
	cfg := testConfig(1, 3)
	cfg.SnapshotThreshold = 1
	stable := NewFileStableStore(filepath.Join(t.TempDir(), "raft-state.json"))
	logStore := newMemoryLogStore()
	memory := store.NewMemoryStore(20)
	r, err := NewWithSnapshot(
		cfg,
		stable,
		logStore,
		memoryStoreApplier{store: memory},
		memory,
		failingSnapshotStore{err: errors.New("disk full")},
		newLocalTransport(),
	)
	if err != nil {
		t.Fatal(err)
	}
	r.Start(context.Background())
	t.Cleanup(r.Stop)

	response := r.HandleAppendEntries(context.Background(), AppendEntriesRequest{
		Term:         1,
		LeaderID:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{Index: 1, Term: 1, Operation: SetOperation, Key: "a", Value: "one"},
		},
		LeaderCommit: 1,
	})
	if !response.Success {
		t.Fatalf("AppendEntries() = %+v, want success", response)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if r.SnapshotError() != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if r.SnapshotError() == nil {
		t.Fatal("snapshot save failure was not recorded")
	}
	if metadata := r.SnapshotMetadata(); metadata.LastIncludedIndex != 0 {
		t.Fatalf("snapshot metadata = %+v after failed save, want empty boundary", metadata)
	}
	entries, err := logStore.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Index != 1 {
		t.Fatalf("log after failed snapshot save = %+v, want original entry", entries)
	}
}

func TestAppliedEntriesAreSnapshottedAndOnlySuffixRemains(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(1, 3)
	cfg.SnapshotThreshold = 2
	stable := NewFileStableStore(filepath.Join(dir, "raft-state.json"))
	logStore, err := NewFileLogStore(filepath.Join(dir, "raft-log.wal"))
	if err != nil {
		t.Fatal(err)
	}
	memory := store.NewMemoryStore(20)
	snapshotStore := snapshot.NewFileStore(filepath.Join(dir, "raft-snapshot.json"))
	r, err := NewWithSnapshot(cfg, stable, logStore, memoryStoreApplier{store: memory}, memory, snapshotStore, newLocalTransport())
	if err != nil {
		_ = logStore.Close()
		t.Fatal(err)
	}
	r.Start(context.Background())
	t.Cleanup(r.Stop)
	t.Cleanup(func() { _ = logStore.Close() })

	response := r.HandleAppendEntries(context.Background(), AppendEntriesRequest{
		Term:         1,
		LeaderID:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{Index: 1, Term: 1, Operation: SetOperation, Key: "a", Value: "one"},
			{Index: 2, Term: 1, Operation: SetOperation, Key: "b", Value: "two"},
			{Index: 3, Term: 1, Operation: SetOperation, Key: "uncommitted", Value: "hidden"},
		},
		LeaderCommit: 2,
	})
	if !response.Success {
		t.Fatalf("AppendEntries() = %+v, want success", response)
	}

	waitForSnapshotIndex(t, r, 2)
	if r.LastApplied() != 2 || r.CommitIndex() != 2 {
		t.Fatalf("applied/committed = %d/%d, want 2/2", r.LastApplied(), r.CommitIndex())
	}
	storedSnapshot, found, err := snapshotStore.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !found || storedSnapshot.LastIncludedIndex != 2 || storedSnapshot.LastIncludedTerm != 1 {
		t.Fatalf("snapshot = %+v, found=%t; want boundary 2/1", storedSnapshot, found)
	}
	snapshotState := store.NewMemoryStore(20)
	if err := snapshotState.Restore(storedSnapshot.Data); err != nil {
		t.Fatal(err)
	}
	if _, err := snapshotState.Get("uncommitted"); err == nil {
		t.Fatal("snapshot contained an entry that was not committed and applied")
	}
	if value, err := memory.Get("a"); err != nil || value != "one" {
		t.Fatalf("state-machine value a = %q, %v; want one, nil", value, err)
	}
	persistedLog, err := logStore.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(persistedLog) != 1 || persistedLog[0].Index != 3 {
		t.Fatalf("durable suffix = %+v, want only uncommitted index 3", persistedLog)
	}

	// Simulate a fresh process after successful snapshot publication and log
	// compaction. The snapshot rebuilds a/b; index 3 stays durable but hidden.
	r.Stop()
	if err := logStore.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedLog, err := NewFileLogStore(filepath.Join(dir, "raft-log.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopenedLog.Close() })
	recoveredMemory := store.NewMemoryStore(20)
	recovered, err := NewWithSnapshot(cfg, stable, reopenedLog, memoryStoreApplier{store: recoveredMemory}, recoveredMemory, snapshotStore, nil)
	if err != nil {
		t.Fatal(err)
	}
	if metadata := recovered.SnapshotMetadata(); metadata.LastIncludedIndex != 2 || metadata.LastIncludedTerm != 1 {
		t.Fatalf("recovered snapshot metadata = %+v, want 2/1", metadata)
	}
	if value, err := recoveredMemory.Get("b"); err != nil || value != "two" {
		t.Fatalf("recovered snapshot value b = %q, %v; want two, nil", value, err)
	}
	if _, err := recoveredMemory.Get("uncommitted"); err == nil {
		t.Fatal("uncommitted suffix entry became visible after restart")
	}
}

func TestStartupCompactsRedundantPrefixAfterSnapshotWasSaved(t *testing.T) {
	dir := t.TempDir()
	stable := NewFileStableStore(filepath.Join(dir, "raft-state.json"))
	if err := stable.Save(StableState{CurrentTerm: 1, VotedFor: NoVote, CommitIndex: 2, LastApplied: 2}); err != nil {
		t.Fatal(err)
	}

	beforeCrash := store.NewMemoryStore(20)
	if err := beforeCrash.Set("a", "one"); err != nil {
		t.Fatal(err)
	}
	if err := beforeCrash.Set("b", "two"); err != nil {
		t.Fatal(err)
	}
	data, err := beforeCrash.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	snapshotStore := snapshot.NewFileStore(filepath.Join(dir, "raft-snapshot.json"))
	if err := snapshotStore.Save(snapshot.Snapshot{LastIncludedIndex: 2, LastIncludedTerm: 1, Data: data}); err != nil {
		t.Fatal(err)
	}

	// This is the crash window: Save() finished but CompactThrough() never ran.
	logStore, err := NewFileLogStore(filepath.Join(dir, "raft-log.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = logStore.Close() })
	if err := logStore.Append([]LogEntry{
		{Index: 1, Term: 1, Operation: SetOperation, Key: "a", Value: "one"},
		{Index: 2, Term: 1, Operation: SetOperation, Key: "b", Value: "two"},
		{Index: 3, Term: 1, Operation: SetOperation, Key: "uncommitted", Value: "hidden"},
	}); err != nil {
		t.Fatal(err)
	}

	recoveredMemory := store.NewMemoryStore(20)
	recovered, err := NewWithSnapshot(testConfig(1, 3), stable, logStore, memoryStoreApplier{store: recoveredMemory}, recoveredMemory, snapshotStore, nil)
	if err != nil {
		t.Fatal(err)
	}
	if metadata := recovered.SnapshotMetadata(); metadata.LastIncludedIndex != 2 {
		t.Fatalf("snapshot metadata index = %d, want 2", metadata.LastIncludedIndex)
	}
	remaining, err := logStore.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 1 || remaining[0].Index != 3 {
		t.Fatalf("recovered log suffix = %+v, want only index 3", remaining)
	}
	if value, err := recoveredMemory.Get("a"); err != nil || value != "one" {
		t.Fatalf("restored state a = %q, %v; want one, nil", value, err)
	}
}

func TestCompactionFailureRecoversFromDurableSnapshotAndFullLog(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(1, 3)
	cfg.SnapshotThreshold = 1
	stable := NewFileStableStore(filepath.Join(dir, "raft-state.json"))
	baseLog, err := NewFileLogStore(filepath.Join(dir, "raft-log.wal"))
	if err != nil {
		t.Fatal(err)
	}
	snapshotStore := snapshot.NewFileStore(filepath.Join(dir, "raft-snapshot.json"))
	memory := store.NewMemoryStore(20)
	r, err := NewWithSnapshot(
		cfg,
		stable,
		failingCompactLogStore{LogStore: baseLog, err: errors.New("compaction interrupted")},
		memoryStoreApplier{store: memory},
		memory,
		snapshotStore,
		newLocalTransport(),
	)
	if err != nil {
		_ = baseLog.Close()
		t.Fatal(err)
	}
	r.Start(context.Background())

	response := r.HandleAppendEntries(context.Background(), AppendEntriesRequest{
		Term:         1,
		LeaderID:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{Index: 1, Term: 1, Operation: SetOperation, Key: "survives", Value: "yes"},
		},
		LeaderCommit: 1,
	})
	if !response.Success {
		t.Fatalf("AppendEntries() = %+v, want success", response)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if r.SnapshotError() != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if r.SnapshotError() == nil {
		t.Fatal("compaction failure was not recorded")
	}
	if _, found, err := snapshotStore.Load(); err != nil || !found {
		t.Fatalf("snapshot after compaction failure: found=%t err=%v; want durable snapshot", found, err)
	}
	entries, err := baseLog.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Index != 1 {
		t.Fatalf("full log after failed compaction = %+v, want index 1", entries)
	}

	// This models a crash at the exact boundary: the new snapshot is durable,
	// but the old log prefix is still present. Startup must finish compaction
	// and rebuild exactly the snapshot state.
	r.Stop()
	if err := baseLog.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedLog, err := NewFileLogStore(filepath.Join(dir, "raft-log.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopenedLog.Close() })
	recoveredMemory := store.NewMemoryStore(20)
	recovered, err := NewWithSnapshot(cfg, stable, reopenedLog, memoryStoreApplier{store: recoveredMemory}, recoveredMemory, snapshotStore, nil)
	if err != nil {
		t.Fatal(err)
	}
	if metadata := recovered.SnapshotMetadata(); metadata.LastIncludedIndex != 1 || metadata.LastIncludedTerm != 1 {
		t.Fatalf("recovered snapshot metadata = %+v, want 1/1", metadata)
	}
	if value, err := recoveredMemory.Get("survives"); err != nil || value != "yes" {
		t.Fatalf("recovered snapshot value = %q, %v; want yes, nil", value, err)
	}
	remaining, err := reopenedLog.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 0 {
		t.Fatalf("recovery did not compact snapshot prefix: %+v", remaining)
	}
}

func TestNewWithSnapshotRejectsCorruptedSnapshot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raft-snapshot.json")
	snapshotStore := snapshot.NewFileStore(path)
	if err := snapshotStore.Save(snapshot.Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 1, Data: []byte(`{"a":"one"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("this is not a snapshot"), 0o600); err != nil {
		t.Fatal(err)
	}

	stateMachine := store.NewMemoryStore(20)
	_, err := NewWithSnapshot(
		testConfig(1, 3),
		NewFileStableStore(filepath.Join(dir, "raft-state.json")),
		newMemoryLogStore(),
		memoryStoreApplier{store: stateMachine},
		stateMachine,
		snapshotStore,
		nil,
	)
	if err == nil {
		t.Fatal("NewWithSnapshot() accepted corrupted snapshot bytes")
	}
}

func waitForSnapshotIndex(t *testing.T, r *Raft, index uint64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if r.SnapshotMetadata().LastIncludedIndex == index {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("snapshot did not reach index %d: metadata=%+v applied=%d", index, r.SnapshotMetadata(), r.LastApplied())
}
