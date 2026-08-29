package raft

import (
	"encoding/json"
	"fmt"

	"github.com/pranavbhole123/distributed_kv_store/internal/wal"
)

type Operation string

const (
	SetOperation    Operation = "SET"
	DeleteOperation Operation = "DELETE"
)

// LogEntry is a command accepted by Raft. Index starts at 1; index 0 is the
// implicit empty entry used by Raft's log-matching rules.
type LogEntry struct {
	Index     uint64    `json:"index"`
	Term      uint64    `json:"term"`
	Operation Operation `json:"operation"`
	Key       string    `json:"key"`
	Value     string    `json:"value,omitempty"`
}

func (e LogEntry) Validate() error {
	if e.Index == 0 {
		return fmt.Errorf("log entry index must be positive")
	}
	if e.Key == "" {
		return fmt.Errorf("log entry key cannot be empty")
	}
	switch e.Operation {
	case SetOperation:
	case DeleteOperation:
		if e.Value != "" {
			return fmt.Errorf("DELETE entry %d cannot contain a value", e.Index)
		}
	default:
		return fmt.Errorf("log entry %d has unknown operation %q", e.Index, e.Operation)
	}
	return nil
}

// LogStore is the durable source of Raft entries. Phase 4.2 adds truncation
// and replacement methods for resolving conflicting follower suffixes.
type LogStore interface {
	Load() ([]LogEntry, error)
	Append(LogEntry) error
	Close() error
}

type FileLogStore struct {
	wal *wal.WAL
}

func NewFileLogStore(path string) (*FileLogStore, error) {
	w, err := wal.NewWAL(path)
	if err != nil {
		return nil, err
	}
	return &FileLogStore{wal: w}, nil
}

func (s *FileLogStore) Append(entry LogEntry) error {
	if err := entry.Validate(); err != nil {
		return err
	}
	record, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("encode log entry %d: %w", entry.Index, err)
	}
	if err := s.wal.Append(record); err != nil {
		return fmt.Errorf("append log entry %d: %w", entry.Index, err)
	}
	return nil
}

func (s *FileLogStore) Load() ([]LogEntry, error) {
	records, err := s.wal.Replay()
	if err != nil {
		return nil, err
	}
	entries := make([]LogEntry, 0, len(records))
	for position, record := range records {
		var entry LogEntry
		if err := json.Unmarshal(record, &entry); err != nil {
			return nil, fmt.Errorf("decode log entry at record %d: %w", position+1, err)
		}
		if err := entry.Validate(); err != nil {
			return nil, err
		}
		if entry.Index != uint64(position+1) {
			return nil, fmt.Errorf("log entry index %d at record %d is not contiguous", entry.Index, position+1)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (s *FileLogStore) Close() error {
	return s.wal.Close()
}
