package raft

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFileLogStoreLoadsDurableEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-log.wal")
	store, err := NewFileLogStore(path)
	if err != nil {
		t.Fatal(err)
	}
	entry := LogEntry{Index: 1, Term: 3, Operation: SetOperation, Key: "name", Value: "pranav"}
	if err := store.Append([]LogEntry{entry}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	afterRestart, err := NewFileLogStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = afterRestart.Close() })
	entries, err := afterRestart.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0] != entry {
		t.Fatalf("Load() = %+v, want [%+v]", entries, entry)
	}
}

func TestFileLogStoreDiscardsTornFinalRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-log.wal")
	store, err := NewFileLogStore(path)
	if err != nil {
		t.Fatal(err)
	}
	entry := LogEntry{Index: 1, Term: 1, Operation: SetOperation, Key: "safe", Value: "entry"}
	if err := store.Append([]LogEntry{entry}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	afterRestart, err := NewFileLogStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = afterRestart.Close() })
	entries, err := afterRestart.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0] != entry {
		t.Fatalf("Load() after torn append = %+v, want [%+v]", entries, entry)
	}
}

func TestFileLogStoreSupportsCompactedAbsoluteSuffix(t *testing.T) {
	store, err := NewFileLogStore(filepath.Join(t.TempDir(), "raft-log.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	entries := []LogEntry{
		{Index: 501, Term: 7, Operation: SetOperation, Key: "a", Value: "1"},
		{Index: 502, Term: 8, Operation: SetOperation, Key: "b", Value: "2"},
	}
	if err := store.Append(entries); err != nil {
		t.Fatal(err)
	}
	if err := store.TruncateFrom(502); err != nil {
		t.Fatal(err)
	}
	got, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != entries[0] {
		t.Fatalf("compacted suffix after truncate = %+v, want %+v", got, entries[:1])
	}
}

func TestFileLogStoreCompactThroughKeepsOnlySuffix(t *testing.T) {
	store, err := NewFileLogStore(filepath.Join(t.TempDir(), "raft-log.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	entries := []LogEntry{
		{Index: 1, Term: 1, Operation: SetOperation, Key: "a", Value: "1"},
		{Index: 2, Term: 1, Operation: SetOperation, Key: "b", Value: "2"},
		{Index: 3, Term: 2, Operation: SetOperation, Key: "c", Value: "3"},
	}
	if err := store.Append(entries); err != nil {
		t.Fatal(err)
	}
	if err := store.CompactThrough(2); err != nil {
		t.Fatal(err)
	}
	got, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != entries[2] {
		t.Fatalf("compacted log = %+v, want [%+v]", got, entries[2])
	}
}
