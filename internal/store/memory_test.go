package store

import "testing"

func TestDeleteMissingKeyIsIdempotent(t *testing.T) {
	memory := NewMemoryStore(10)
	if err := memory.Delete("missing"); err != nil {
		t.Fatalf("Delete(missing) = %v, want nil", err)
	}
}
