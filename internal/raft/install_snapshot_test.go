package raft

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/pranavbhole123/distributed_kv_store/internal/snapshot"
	"github.com/pranavbhole123/distributed_kv_store/internal/store"
)

func TestInstallSnapshotDiscardsConflictingSuffix(t *testing.T) {
	dir := t.TempDir()
	stable := NewFileStableStore(filepath.Join(dir, "raft-state.json"))
	if err := stable.Save(StableState{CurrentTerm: 1, VotedFor: NoVote, CommitIndex: 1, LastApplied: 1}); err != nil {
		t.Fatal(err)
	}
	logStore := newMemoryLogStore()
	if err := logStore.Append([]LogEntry{
		{Index: 1, Term: 1, Operation: SetOperation, Key: "old", Value: "one"},
		{Index: 2, Term: 1, Operation: SetOperation, Key: "conflict", Value: "old"},
		{Index: 3, Term: 1, Operation: SetOperation, Key: "also-conflicting", Value: "old"},
	}); err != nil {
		t.Fatal(err)
	}
	followerStore := store.NewMemoryStore(20)
	follower, err := NewWithSnapshot(testConfig(2, 3), stable, logStore, memoryStoreApplier{store: followerStore}, followerStore, snapshot.NewFileStore(filepath.Join(dir, "snapshot.json")), newLocalTransport())
	if err != nil {
		t.Fatal(err)
	}

	leaderStore := store.NewMemoryStore(20)
	if err := leaderStore.Set("from-snapshot", "new"); err != nil {
		t.Fatal(err)
	}
	data, err := leaderStore.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	response, err := follower.HandleInstallSnapshot(context.Background(), InstallSnapshotRequest{
		Term:              2,
		LeaderID:          1,
		LastIncludedIndex: 2,
		LastIncludedTerm:  2,
		Data:              data,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Term != 2 {
		t.Fatalf("response term = %d, want 2", response.Term)
	}
	if metadata := follower.SnapshotMetadata(); metadata.LastIncludedIndex != 2 || metadata.LastIncludedTerm != 2 {
		t.Fatalf("snapshot metadata = %+v, want 2/2", metadata)
	}
	if follower.CommitIndex() != 2 || follower.LastApplied() != 2 {
		t.Fatalf("commit/applied = %d/%d, want 2/2", follower.CommitIndex(), follower.LastApplied())
	}
	if entries, err := logStore.Load(); err != nil || len(entries) != 0 {
		t.Fatalf("conflicting durable suffix = %+v, %v; want empty", entries, err)
	}
	if value, err := followerStore.Get("from-snapshot"); err != nil || value != "new" {
		t.Fatalf("restored value = %q, %v; want new, nil", value, err)
	}
	if _, err := followerStore.Get("old"); err == nil {
		t.Fatal("pre-snapshot state survived Restore")
	}
}

func TestInstallSnapshotRetainsMatchingSuffixAndAppliesIt(t *testing.T) {
	dir := t.TempDir()
	stable := NewFileStableStore(filepath.Join(dir, "raft-state.json"))
	if err := stable.Save(StableState{CurrentTerm: 1, VotedFor: NoVote, CommitIndex: 1, LastApplied: 1}); err != nil {
		t.Fatal(err)
	}
	logStore := newMemoryLogStore()
	if err := logStore.Append([]LogEntry{
		{Index: 1, Term: 1, Operation: SetOperation, Key: "old", Value: "one"},
		{Index: 2, Term: 2, Operation: SetOperation, Key: "from-snapshot", Value: "two"},
		{Index: 3, Term: 2, Operation: SetOperation, Key: "suffix", Value: "three"},
	}); err != nil {
		t.Fatal(err)
	}
	followerStore := store.NewMemoryStore(20)
	follower, err := NewWithSnapshot(testConfig(2, 3), stable, logStore, memoryStoreApplier{store: followerStore}, followerStore, snapshot.NewFileStore(filepath.Join(dir, "snapshot.json")), newLocalTransport())
	if err != nil {
		t.Fatal(err)
	}
	follower.Start(context.Background())
	t.Cleanup(follower.Stop)

	leaderStore := store.NewMemoryStore(20)
	if err := leaderStore.Set("from-snapshot", "two"); err != nil {
		t.Fatal(err)
	}
	data, err := leaderStore.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := follower.HandleInstallSnapshot(context.Background(), InstallSnapshotRequest{
		Term:              2,
		LeaderID:          1,
		LastIncludedIndex: 2,
		LastIncludedTerm:  2,
		Data:              data,
	}); err != nil {
		t.Fatal(err)
	}
	response := follower.HandleAppendEntries(context.Background(), AppendEntriesRequest{
		Term:         2,
		LeaderID:     1,
		PrevLogIndex: 2,
		PrevLogTerm:  2,
		LeaderCommit: 3,
	})
	if !response.Success {
		t.Fatalf("AppendEntries after snapshot = %+v, want success", response)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if follower.LastApplied() == 3 {
			entries, err := logStore.Load()
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 1 || entries[0].Index != 3 {
				t.Fatalf("retained durable suffix = %+v, want only index 3", entries)
			}
			if value, err := followerStore.Get("suffix"); err != nil || value != "three" {
				t.Fatalf("applied retained suffix = %q, %v; want three, nil", value, err)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("retained suffix was not applied: commit=%d applied=%d", follower.CommitIndex(), follower.LastApplied())
}

func TestLeaderInstallsSnapshotThenReplicatesSuffix(t *testing.T) {
	dir := t.TempDir()
	leaderSnapshotStore := snapshot.NewFileStore(filepath.Join(dir, "leader-snapshot.json"))
	leaderStateMachine := store.NewMemoryStore(20)
	if err := leaderStateMachine.Set("snap", "state"); err != nil {
		t.Fatal(err)
	}
	snapshotData, err := leaderStateMachine.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := leaderSnapshotStore.Save(snapshot.Snapshot{LastIncludedIndex: 2, LastIncludedTerm: 1, Data: snapshotData}); err != nil {
		t.Fatal(err)
	}
	leaderStable := NewFileStableStore(filepath.Join(dir, "leader-state.json"))
	if err := leaderStable.Save(StableState{CurrentTerm: 2, VotedFor: NoVote, CommitIndex: 3, LastApplied: 2}); err != nil {
		t.Fatal(err)
	}
	leaderLog := newMemoryLogStore()
	if err := leaderLog.Append([]LogEntry{{Index: 3, Term: 2, Operation: SetOperation, Key: "suffix", Value: "replicated"}}); err != nil {
		t.Fatal(err)
	}
	transport := newLocalTransport()
	leader, err := NewWithSnapshot(testConfig(1, 3), leaderStable, leaderLog, memoryStoreApplier{store: leaderStateMachine}, leaderStateMachine, leaderSnapshotStore, transport)
	if err != nil {
		t.Fatal(err)
	}

	followerStateMachine := store.NewMemoryStore(20)
	follower, err := NewWithSnapshot(
		testConfig(2, 3),
		NewFileStableStore(filepath.Join(dir, "follower-state.json")),
		newMemoryLogStore(),
		memoryStoreApplier{store: followerStateMachine},
		followerStateMachine,
		snapshot.NewFileStore(filepath.Join(dir, "follower-snapshot.json")),
		transport,
	)
	if err != nil {
		t.Fatal(err)
	}
	transport.register(follower)
	follower.Start(context.Background())
	t.Cleanup(follower.Stop)

	leader.mu.Lock()
	leader.state = Leader
	leader.leaderID = leader.id
	leader.nextIndex[follower.ID()] = 1
	leader.matchIndex[follower.ID()] = 0
	leader.mu.Unlock()
	leader.replicateToPeer(context.Background(), follower.ID())

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if follower.LastApplied() == 3 {
			if metadata := follower.SnapshotMetadata(); metadata.LastIncludedIndex != 2 || metadata.LastIncludedTerm != 1 {
				t.Fatalf("follower snapshot metadata = %+v, want 2/1", metadata)
			}
			if value, err := followerStateMachine.Get("snap"); err != nil || value != "state" {
				t.Fatalf("follower snapshot state = %q, %v; want state, nil", value, err)
			}
			if value, err := followerStateMachine.Get("suffix"); err != nil || value != "replicated" {
				t.Fatalf("follower suffix state = %q, %v; want replicated, nil", value, err)
			}
			leader.mu.RLock()
			next := leader.nextIndex[follower.ID()]
			matched := leader.matchIndex[follower.ID()]
			leader.mu.RUnlock()
			if next != 4 || matched != 3 {
				t.Fatalf("leader replication state next=%d match=%d, want 4/3", next, matched)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("follower did not install and apply suffix: commit=%d applied=%d", follower.CommitIndex(), follower.LastApplied())
}
