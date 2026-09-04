package snapshot

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestFileStoreRoundTrip(t *testing.T) {
	store := NewFileStore(filepath.Join(t.TempDir(), "raft-snapshot.json"))
	want := Snapshot{
		LastIncludedIndex: 42,
		LastIncludedTerm:  7,
		Data:              []byte(`{"answer":"42"}`),
	}
	if err := store.Save(want); err != nil {
		t.Fatal(err)
	}

	got, found, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("Load() found=false, want saved snapshot")
	}
	if got.LastIncludedIndex != want.LastIncludedIndex || got.LastIncludedTerm != want.LastIncludedTerm || string(got.Data) != string(want.Data) {
		t.Fatalf("Load() = %+v, want %+v", got, want)
	}
}

func TestFileStoreMissingSnapshotIsNotAnError(t *testing.T) {
	store := NewFileStore(filepath.Join(t.TempDir(), "missing.json"))
	_, found, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("Load() found a snapshot that was never saved")
	}
}

func TestFileStoreRejectsChecksumMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-snapshot.json")
	store := NewFileStore(path)
	if err := store.Save(Snapshot{LastIncludedIndex: 3, LastIncludedTerm: 1, Data: []byte(`{"a":"b"}`)}); err != nil {
		t.Fatal(err)
	}

	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(encoded, &document); err != nil {
		t.Fatal(err)
	}
	document["checksum"] = "not-the-real-checksum"
	encoded, err = json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0644); err != nil {
		t.Fatal(err)
	}

	if _, _, err := store.Load(); err == nil {
		t.Fatal("Load() succeeded with a corrupt checksum")
	}
}
