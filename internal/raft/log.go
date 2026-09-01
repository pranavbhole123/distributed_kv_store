package raft

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/pranavbhole123/distributed_kv_store/internal/wal"
)

type Operation string

const (
	// NoopOperation is an internal Raft entry appended by a newly elected
	// leader. It maps to protobuf's zero/unspecified enum value and is never a
	// client command.
	NoopOperation   Operation = ""
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
	switch e.Operation {
	case NoopOperation:
		if e.Key != "" || e.Value != "" {
			return fmt.Errorf("NOOP entry %d cannot contain a key or value", e.Index)
		}
	case SetOperation:
		if e.Key == "" {
			return fmt.Errorf("SET entry %d key cannot be empty", e.Index)
		}
	case DeleteOperation:
		if e.Key == "" {
			return fmt.Errorf("DELETE entry %d key cannot be empty", e.Index)
		}
		if e.Value != "" {
			return fmt.Errorf("DELETE entry %d cannot contain a value", e.Index)
		}
	default:
		return fmt.Errorf("log entry %d has unknown operation %q", e.Index, e.Operation)
	}
	return nil
}

// LogStore is the durable source of Raft entries. Normal replication appends
// batches; only a conflicting uncommitted suffix requires truncation.
type LogStore interface {
	Load() ([]LogEntry, error)
	Append([]LogEntry) error
	TruncateFrom(index uint64) error
	Close() error
}

type FileLogStore struct {
	wal *wal.WAL
	mu  sync.Mutex
}

func NewFileLogStore(path string) (*FileLogStore, error) {
	w, err := wal.NewWAL(path)
	if err != nil {
		return nil, err
	}
	return &FileLogStore{wal: w}, nil
}

func (s *FileLogStore) Append(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	records, err := encodeEntries(entries)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.wal.AppendBatch(records); err != nil {
		return fmt.Errorf("append Raft log entries: %w", err)
	}
	return nil
}

func (s *FileLogStore) Load() ([]LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadLocked()
}

func (s *FileLogStore) loadLocked() ([]LogEntry, error) {
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

// TruncateFrom deletes index and every later entry. It atomically rewrites the
// retained prefix because conflicting suffixes are rare compared with appends.
func (s *FileLogStore) TruncateFrom(index uint64) error {
	if index == 0 {
		return fmt.Errorf("truncate index must be positive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := s.loadLocked()
	if err != nil {
		return err
	}
	if index > uint64(len(entries)+1) {
		return fmt.Errorf("truncate index %d exceeds log length %d", index, len(entries))
	}
	return s.replaceLocked(entries[:index-1])
}

func (s *FileLogStore) replaceLocked(entries []LogEntry) error {
	records := make([][]byte, 0, len(entries))
	for position, entry := range entries {
		if err := entry.Validate(); err != nil {
			return err
		}
		if entry.Index != uint64(position+1) {
			return fmt.Errorf("replacement entry index %d at position %d is not contiguous", entry.Index, position+1)
		}
		record, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("encode replacement entry %d: %w", entry.Index, err)
		}
		records = append(records, record)
	}
	if err := s.wal.Replace(records); err != nil {
		return fmt.Errorf("replace Raft log: %w", err)
	}
	return nil
}

func encodeEntries(entries []LogEntry) ([][]byte, error) {
	records := make([][]byte, 0, len(entries))
	for position, entry := range entries {
		if err := entry.Validate(); err != nil {
			return nil, err
		}
		if position > 0 && entry.Index != entries[position-1].Index+1 {
			return nil, fmt.Errorf("entry index %d does not follow index %d", entry.Index, entries[position-1].Index)
		}
		record, err := json.Marshal(entry)
		if err != nil {
			return nil, fmt.Errorf("encode log entry %d: %w", entry.Index, err)
		}
		records = append(records, record)
	}
	return records, nil
}

func (s *FileLogStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wal.Close()
}
