package raft

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestCompactedSuffixUsesAbsoluteIndexesForReplication(t *testing.T) {
	meta := SnapshotMetadata{LastIncludedIndex: 500, LastIncludedTerm: 7}
	stable := NewFileStableStore(filepath.Join(t.TempDir(), "raft-state.json"))
	if err := stable.Save(StableState{CurrentTerm: 8, VotedFor: NoVote, CommitIndex: 500, LastApplied: 500}); err != nil {
		t.Fatal(err)
	}
	logStore := newMemoryLogStore()
	if err := logStore.Append([]LogEntry{
		{Index: 501, Term: 7, Operation: SetOperation, Key: "a", Value: "1"},
		{Index: 502, Term: 8, Operation: SetOperation, Key: "b", Value: "2"},
	}); err != nil {
		t.Fatal(err)
	}
	r, err := newWithLogAndSnapshot(testConfig(1, 3), stable, logStore, nil, newLocalTransport(), meta, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	r.mu.Lock()
	r.state = Leader
	r.leaderID = r.id
	r.nextIndex[2] = 501
	r.matchIndex[2] = 0
	r.mu.Unlock()

	request, ok := r.buildAppendEntries(2)
	if !ok {
		t.Fatal("could not build AppendEntries from compacted suffix")
	}
	if request.PrevLogIndex != 500 || request.PrevLogTerm != 7 {
		t.Fatalf("previous log = %d/%d, want snapshot boundary 500/7", request.PrevLogIndex, request.PrevLogTerm)
	}
	if len(request.Entries) != 2 || request.Entries[0].Index != 501 || request.Entries[1].Index != 502 {
		t.Fatalf("entries = %+v, want absolute suffix [501, 502]", request.Entries)
	}
	if index, term := r.lastLogInfoLocked(); index != 502 || term != 8 {
		t.Fatalf("last log = %d/%d, want 502/8", index, term)
	}
	if !r.matchesPrefixLocked(500, 7) || !r.matchesPrefixLocked(501, 7) || r.matchesPrefixLocked(500, 8) {
		t.Fatal("snapshot-boundary prefix matching is incorrect")
	}
}

func TestFollowerAppliesAbsoluteSuffixAfterSnapshotBoundary(t *testing.T) {
	meta := SnapshotMetadata{LastIncludedIndex: 500, LastIncludedTerm: 7}
	stable := NewFileStableStore(filepath.Join(t.TempDir(), "raft-state.json"))
	if err := stable.Save(StableState{CurrentTerm: 8, VotedFor: NoVote, CommitIndex: 500, LastApplied: 500}); err != nil {
		t.Fatal(err)
	}
	logStore := newMemoryLogStore()
	if err := logStore.Append([]LogEntry{
		{Index: 501, Term: 7, Operation: SetOperation, Key: "a", Value: "1"},
	}); err != nil {
		t.Fatal(err)
	}
	applier := &orderedApplier{}
	follower, err := newWithLogAndSnapshot(testConfig(2, 3), stable, logStore, applier, newLocalTransport(), meta, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	follower.Start(context.Background())
	t.Cleanup(follower.Stop)

	response := follower.HandleAppendEntries(context.Background(), AppendEntriesRequest{
		Term:         8,
		LeaderID:     1,
		PrevLogIndex: 501,
		PrevLogTerm:  7,
		Entries: []LogEntry{
			{Index: 502, Term: 8, Operation: SetOperation, Key: "b", Value: "2"},
		},
		LeaderCommit: 502,
	})
	if !response.Success {
		t.Fatalf("AppendEntries() = %+v, want success", response)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if follower.LastApplied() == 502 {
			entries := applier.Entries()
			if len(entries) != 2 || entries[0].Index != 501 || entries[1].Index != 502 {
				t.Fatalf("applied entries = %+v, want absolute indexes [501, 502]", entries)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("follower did not apply absolute suffix: commit=%d applied=%d", follower.CommitIndex(), follower.LastApplied())
}
