package raft

import "sync"

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

func (s *memoryLogStore) Append(entry LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, entry)
	return nil
}

func (s *memoryLogStore) Close() error { return nil }
