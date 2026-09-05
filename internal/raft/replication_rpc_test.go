package raft

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
)

type trackingLogStore struct {
	entries       []LogEntry
	appendCalls   int
	truncateCalls int
}

func (s *trackingLogStore) Load() ([]LogEntry, error) {
	return append([]LogEntry(nil), s.entries...), nil
}

func (s *trackingLogStore) Append(entries []LogEntry) error {
	s.appendCalls++
	s.entries = append(s.entries, entries...)
	return nil
}

func (s *trackingLogStore) TruncateFrom(index uint64) error {
	s.truncateCalls++
	if index == 0 || index > uint64(len(s.entries)+1) {
		return fmt.Errorf("invalid truncate index %d", index)
	}
	s.entries = s.entries[:index-1]
	return nil
}

func (s *trackingLogStore) CompactThrough(index uint64) error {
	firstRetained := len(s.entries)
	for position, entry := range s.entries {
		if entry.Index > index {
			firstRetained = position
			break
		}
	}
	s.entries = s.entries[firstRetained:]
	return nil
}

func (s *trackingLogStore) Close() error { return nil }

func TestRequestVoteRejectsCandidateWithOlderLog(t *testing.T) {
	logStore := newMemoryLogStore()
	entries := []LogEntry{
		{Index: 1, Term: 1, Operation: SetOperation, Key: "a", Value: "1"},
		{Index: 2, Term: 2, Operation: SetOperation, Key: "b", Value: "2"},
	}
	if err := logStore.Append(entries); err != nil {
		t.Fatal(err)
	}
	node, err := NewWithLog(testConfig(1, 3), NewFileStableStore(filepath.Join(t.TempDir(), "raft-state.json")), logStore, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	olderTerm := node.HandleRequestVote(context.Background(), RequestVoteRequest{
		Term: 3, CandidateID: 2, LastLogIndex: 100, LastLogTerm: 1,
	})
	if olderTerm.VoteGranted {
		t.Fatal("granted vote to candidate with older last-log term")
	}
	shorterSameTerm := node.HandleRequestVote(context.Background(), RequestVoteRequest{
		Term: 3, CandidateID: 2, LastLogIndex: 1, LastLogTerm: 2,
	})
	if shorterSameTerm.VoteGranted {
		t.Fatal("granted vote to candidate with shorter same-term log")
	}
	upToDate := node.HandleRequestVote(context.Background(), RequestVoteRequest{
		Term: 3, CandidateID: 2, LastLogIndex: 2, LastLogTerm: 2,
	})
	if !upToDate.VoteGranted {
		t.Fatal("rejected vote to candidate with equally up-to-date log")
	}
}

func TestAppendEntriesReplacesOnlyUncommittedConflictingSuffix(t *testing.T) {
	dir := t.TempDir()
	stable := NewFileStableStore(filepath.Join(dir, "raft-state.json"))
	if err := stable.Save(StableState{CurrentTerm: 2, VotedFor: NoVote, CommitIndex: 2, LastApplied: 2}); err != nil {
		t.Fatal(err)
	}
	logStore, err := NewFileLogStore(filepath.Join(dir, "raft-log.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = logStore.Close() })
	if err := logStore.Append([]LogEntry{
		{Index: 1, Term: 1, Operation: SetOperation, Key: "a", Value: "1"},
		{Index: 2, Term: 1, Operation: SetOperation, Key: "b", Value: "2"},
		{Index: 3, Term: 2, Operation: SetOperation, Key: "x", Value: "old"},
	}); err != nil {
		t.Fatal(err)
	}
	applier := &recordingApplier{}
	node, err := NewWithLog(testConfig(1, 3), stable, logStore, applier, nil)
	if err != nil {
		t.Fatal(err)
	}

	response := node.HandleAppendEntries(context.Background(), AppendEntriesRequest{
		Term:         3,
		LeaderID:     2,
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{Index: 3, Term: 3, Operation: SetOperation, Key: "x", Value: "new"},
		},
		LeaderCommit: 3,
	})
	if !response.Success {
		t.Fatalf("AppendEntries() = %+v, want success", response)
	}
	if node.log[2].Term != 3 || node.log[2].Value != "new" {
		t.Fatalf("log[3] = %+v, want replacement term-3 entry", node.log[2])
	}
	if node.CommitIndex() != 3 || node.LastApplied() != 2 {
		t.Fatalf("commit=%d applied=%d, want newly committed but not yet runtime-applied entry", node.CommitIndex(), node.LastApplied())
	}
	persistedLog, err := logStore.Load()
	if err != nil {
		t.Fatal(err)
	}
	if persistedLog[2] != node.log[2] {
		t.Fatalf("durable log[3] = %+v, want %+v", persistedLog[2], node.log[2])
	}
}

func TestAppendEntriesAppendsNewSuffixWithoutTruncating(t *testing.T) {
	logStore := &trackingLogStore{entries: []LogEntry{
		{Index: 1, Term: 1, Operation: SetOperation, Key: "a", Value: "1"},
	}}
	node, err := NewWithLog(testConfig(1, 3), NewFileStableStore(filepath.Join(t.TempDir(), "raft-state.json")), logStore, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	response := node.HandleAppendEntries(context.Background(), AppendEntriesRequest{
		Term:         1,
		LeaderID:     2,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{Index: 2, Term: 1, Operation: SetOperation, Key: "b", Value: "2"},
		},
	})
	if !response.Success {
		t.Fatalf("AppendEntries() = %+v, want success", response)
	}
	if logStore.appendCalls != 1 || logStore.truncateCalls != 0 {
		t.Fatalf("append calls=%d truncate calls=%d; want 1 append and no truncation", logStore.appendCalls, logStore.truncateCalls)
	}
}

func TestAppendEntriesRejectsMismatchedPrefixWithoutChangingLog(t *testing.T) {
	logStore := newMemoryLogStore()
	entries := []LogEntry{
		{Index: 1, Term: 1, Operation: SetOperation, Key: "a", Value: "1"},
		{Index: 2, Term: 1, Operation: SetOperation, Key: "b", Value: "2"},
		{Index: 3, Term: 2, Operation: SetOperation, Key: "x", Value: "old"},
	}
	if err := logStore.Append(entries); err != nil {
		t.Fatal(err)
	}
	node, err := NewWithLog(testConfig(1, 3), NewFileStableStore(filepath.Join(t.TempDir(), "raft-state.json")), logStore, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	response := node.HandleAppendEntries(context.Background(), AppendEntriesRequest{
		Term:         3,
		LeaderID:     2,
		PrevLogIndex: 2,
		PrevLogTerm:  2, // local index 2 is term 1
		Entries: []LogEntry{
			{Index: 3, Term: 3, Operation: SetOperation, Key: "x", Value: "new"},
		},
	})
	if response.Success {
		t.Fatal("accepted AppendEntries with mismatched previous log term")
	}
	if !reflect.DeepEqual(node.log, entries) {
		t.Fatalf("mismatched prefix changed log to %+v", node.log)
	}
	stored, err := logStore.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(stored, entries) {
		t.Fatalf("mismatched prefix changed durable log to %+v", stored)
	}
}
