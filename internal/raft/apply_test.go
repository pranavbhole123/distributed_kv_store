package raft

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type orderedApplier struct {
	mu      sync.Mutex
	entries []LogEntry
}

func (a *orderedApplier) Apply(entry LogEntry) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.entries = append(a.entries, entry)
	return nil
}

func (a *orderedApplier) Entries() []LogEntry {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]LogEntry(nil), a.entries...)
}

func TestFollowerAppliesOnlyCommittedEntriesInLogOrder(t *testing.T) {
	stable := NewFileStableStore(filepath.Join(t.TempDir(), "raft-state.json"))
	applier := &orderedApplier{}
	r, err := NewWithLog(testConfig(1, 3), stable, newMemoryLogStore(), applier, newLocalTransport())
	if err != nil {
		t.Fatal(err)
	}
	r.Start(context.Background())
	t.Cleanup(r.Stop)

	entries := []LogEntry{
		{Index: 1, Term: 1, Operation: SetOperation, Key: "a", Value: "1"},
		{Index: 2, Term: 1, Operation: SetOperation, Key: "b", Value: "2"},
	}
	response := r.HandleAppendEntries(context.Background(), AppendEntriesRequest{
		Term:         1,
		LeaderID:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      entries,
		LeaderCommit: 0,
	})
	if !response.Success {
		t.Fatalf("append uncommitted entries = %+v, want success", response)
	}
	time.Sleep(30 * time.Millisecond)
	if got := applier.Entries(); len(got) != 0 {
		t.Fatalf("applied uncommitted entries: %+v", got)
	}

	response = r.HandleAppendEntries(context.Background(), AppendEntriesRequest{
		Term:         1,
		LeaderID:     2,
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		LeaderCommit: 2,
	})
	if !response.Success {
		t.Fatalf("advance follower commit index = %+v, want success", response)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if r.LastApplied() == 2 {
			got := applier.Entries()
			if len(got) != 2 || got[0] != entries[0] || got[1] != entries[1] {
				t.Fatalf("applied entries = %+v, want ordered %+v", got, entries)
			}
			persisted, err := stable.Load()
			if err != nil {
				t.Fatal(err)
			}
			if persisted.CommitIndex != 2 || persisted.LastApplied != 2 {
				t.Fatalf("persisted state = %+v, want commit=2 applied=2", persisted)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("follower did not apply committed entries: commit=%d applied=%d", r.CommitIndex(), r.LastApplied())
}
