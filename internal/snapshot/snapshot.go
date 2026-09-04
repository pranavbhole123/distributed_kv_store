// Package snapshot persists an atomic, checksummed state-machine checkpoint.
package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const formatVersion = 1

// Snapshot is a complete state-machine image and the Raft log entry through
// which that image is valid. Data is opaque to this package.
type Snapshot struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

func (s Snapshot) Validate() error {
	if s.LastIncludedIndex == 0 && s.LastIncludedTerm != 0 {
		return fmt.Errorf("empty snapshot cannot have last included term %d", s.LastIncludedTerm)
	}
	if len(s.Data) == 0 {
		return errors.New("snapshot data cannot be empty")
	}
	return nil
}

// Store is intentionally independent from Raft and the KV store. A missing
// snapshot is represented by found=false rather than an error.
type Store interface {
	Load() (snapshot Snapshot, found bool, err error)
	Save(Snapshot) error
}

// FileStore stores exactly one atomically replaced snapshot per node.
type FileStore struct {
	path string
	mu   sync.Mutex
}

func NewFileStore(path string) *FileStore {
	return &FileStore{path: path}
}

type diskSnapshot struct {
	Version           uint32 `json:"version"`
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
	Data              []byte `json:"data"`
	Checksum          string `json:"checksum"`
}

type checksumPayload struct {
	Version           uint32 `json:"version"`
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
	Data              []byte `json:"data"`
}

func (s *FileStore) Load() (Snapshot, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	file, err := os.Open(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return Snapshot{}, false, nil
	}
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("open snapshot %q: %w", s.path, err)
	}
	defer file.Close()

	var disk diskSnapshot
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&disk); err != nil {
		return Snapshot{}, false, fmt.Errorf("decode snapshot %q: %w", s.path, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return Snapshot{}, false, fmt.Errorf("decode snapshot %q: contains multiple JSON values", s.path)
		}
		return Snapshot{}, false, fmt.Errorf("decode snapshot %q: %w", s.path, err)
	}
	if disk.Version != formatVersion {
		return Snapshot{}, false, fmt.Errorf("snapshot %q has unsupported format version %d", s.path, disk.Version)
	}

	result := Snapshot{
		LastIncludedIndex: disk.LastIncludedIndex,
		LastIncludedTerm:  disk.LastIncludedTerm,
		Data:              append([]byte(nil), disk.Data...),
	}
	if err := result.Validate(); err != nil {
		return Snapshot{}, false, fmt.Errorf("invalid snapshot %q: %w", s.path, err)
	}
	checksum, err := checksumFor(result)
	if err != nil {
		return Snapshot{}, false, err
	}
	if disk.Checksum != checksum {
		return Snapshot{}, false, fmt.Errorf("snapshot %q checksum mismatch", s.path)
	}
	return result, true, nil
}

// Save makes a snapshot durable before atomically publishing it. Syncing the
// directory after rename makes the filename replacement durable too.
func (s *FileStore) Save(snapshot Snapshot) error {
	if err := snapshot.Validate(); err != nil {
		return err
	}
	checksum, err := checksumFor(snapshot)
	if err != nil {
		return err
	}
	disk := diskSnapshot{
		Version:           formatVersion,
		LastIncludedIndex: snapshot.LastIncludedIndex,
		LastIncludedTerm:  snapshot.LastIncludedTerm,
		Data:              snapshot.Data,
		Checksum:          checksum,
	}
	encoded, err := json.Marshal(disk)
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create snapshot directory %q: %w", dir, err)
	}
	temporary, err := os.CreateTemp(dir, ".raft-snapshot-*")
	if err != nil {
		return fmt.Errorf("create temporary snapshot: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)

	if _, err := temporary.Write(encoded); err != nil {
		temporary.Close()
		return fmt.Errorf("write temporary snapshot: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return fmt.Errorf("sync temporary snapshot: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary snapshot: %w", err)
	}
	if err := os.Rename(temporaryPath, s.path); err != nil {
		return fmt.Errorf("replace snapshot %q: %w", s.path, err)
	}
	directory, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open snapshot directory %q: %w", dir, err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync snapshot directory %q: %w", dir, err)
	}
	return nil
}

func checksumFor(snapshot Snapshot) (string, error) {
	payload, err := json.Marshal(checksumPayload{
		Version:           formatVersion,
		LastIncludedIndex: snapshot.LastIncludedIndex,
		LastIncludedTerm:  snapshot.LastIncludedTerm,
		Data:              snapshot.Data,
	})
	if err != nil {
		return "", fmt.Errorf("encode snapshot checksum payload: %w", err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}
