package raft

import (
	"fmt"
	"sync"
)

// memoryLogStore keeps existing election-only tests independent of disk. The
// real node always injects FileLogStore.
type memoryLogStore struct {
	mu      sync.Mutex
	entries []LogEntry
}

func newMemoryLogStore() *memoryLogStore {
	return &memoryLogStore{}
}

func (s *memoryLogStore) Load() ([]LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]LogEntry(nil), s.entries...), nil
}

func (s *memoryLogStore) Append(entries []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, entries...)
	return nil
}

func (s *memoryLogStore) TruncateFrom(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index == 0 || index > uint64(len(s.entries)+1) {
		return fmt.Errorf("invalid truncate index %d", index)
	}
	s.entries = s.entries[:index-1]
	return nil
}

func (s *memoryLogStore) Close() error { return nil }
