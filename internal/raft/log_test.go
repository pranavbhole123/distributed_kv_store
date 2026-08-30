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
