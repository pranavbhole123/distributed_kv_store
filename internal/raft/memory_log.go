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
	if index == 0 {
		return fmt.Errorf("invalid truncate index %d", index)
	}
	if len(s.entries) == 0 {
		return nil
	}
	lastIndex := s.entries[len(s.entries)-1].Index
	if index > lastIndex+1 {
		return fmt.Errorf("invalid truncate index %d after last index %d", index, lastIndex)
	}
	truncateOffset := len(s.entries)
	for position, entry := range s.entries {
		if entry.Index >= index {
			truncateOffset = position
			break
		}
	}
	s.entries = s.entries[:truncateOffset]
	return nil
}

func (s *memoryLogStore) Close() error { return nil }
