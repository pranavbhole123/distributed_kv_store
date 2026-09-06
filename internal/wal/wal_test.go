package wal

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestWALAppendBatchReplayAndReplace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-log.wal")
	w, err := NewWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })

	if err := w.AppendBatch([][]byte{[]byte("one"), []byte("two")}); err != nil {
		t.Fatal(err)
	}
	got, err := w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if want := [][]byte{[]byte("one"), []byte("two")}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Replay() = %q, want %q", got, want)
	}

	if err := w.Replace([][]byte{[]byte("replacement")}); err != nil {
		t.Fatal(err)
	}
	got, err = w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if want := [][]byte{[]byte("replacement")}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Replay() after Replace = %q, want %q", got, want)
	}
}

func TestReplayDiscardsTornFinalRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-log.wal")
	w, err := NewWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]byte("complete")); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a process dying after it writes a record header and only part of
	// that record's payload. Recovery must keep the complete prefix and remove
	// the torn suffix before accepting later appends.
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], 8)
	if _, err := file.Write(header[:]); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("half")); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	got, err := reopened.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if want := [][]byte{[]byte("complete")}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Replay() = %q, want complete prefix %q", got, want)
	}
	if err := reopened.Append([]byte("after-recovery")); err != nil {
		t.Fatal(err)
	}
	got, err = reopened.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if want := [][]byte{[]byte("complete"), []byte("after-recovery")}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Replay() after recovery append = %q, want %q", got, want)
	}
}

func TestReplayRejectsInvalidCompleteRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-log.wal")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	// A complete four-byte header declaring an empty record is corruption, not
	// a crash-torn trailing frame, and must therefore fail recovery.
	if _, err := file.Write([]byte{0, 0, 0, 0}); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	w, err := NewWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	if _, err := w.Replay(); err == nil {
		t.Fatal("Replay() accepted an invalid complete record")
	}
}
