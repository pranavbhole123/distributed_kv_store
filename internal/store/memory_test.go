package store

import "testing"

func TestDeleteMissingKeyIsIdempotent(t *testing.T) {
	memory := NewMemoryStore(10)
	if err := memory.Delete("missing"); err != nil {
		t.Fatalf("Delete(missing) = %v, want nil", err)
	}
}

func TestMemoryStoreSnapshotAndRestoreReplaceWholeMap(t *testing.T) {
	original := NewMemoryStore(10)
	if err := original.Set("kept", "value"); err != nil {
		t.Fatal(err)
	}
	if err := original.Set("another", "two"); err != nil {
		t.Fatal(err)
	}
	data, err := original.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	restored := NewMemoryStore(10)
	if err := restored.Set("stale", "remove-me"); err != nil {
		t.Fatal(err)
	}
	if err := restored.Restore(data); err != nil {
		t.Fatal(err)
	}
	if value, err := restored.Get("kept"); err != nil || value != "value" {
		t.Fatalf("restored kept = %q, %v; want value, nil", value, err)
	}
	if _, err := restored.Get("stale"); err == nil {
		t.Fatal("Restore() merged stale state instead of replacing it")
	}
}

func TestMemoryStoreRestoreLeavesExistingStateOnInvalidData(t *testing.T) {
	memory := NewMemoryStore(3)
	if err := memory.Set("safe", "yes"); err != nil {
		t.Fatal(err)
	}
	if err := memory.Restore([]byte(`{"too-long":"value"}`)); err == nil {
		t.Fatal("Restore() accepted a value longer than the configured limit")
	}
	if value, err := memory.Get("safe"); err != nil || value != "yes" {
		t.Fatalf("state after failed restore = %q, %v; want yes, nil", value, err)
	}
}
