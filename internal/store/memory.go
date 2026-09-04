package store

import (
	"encoding/json"
	"fmt"
	"sync"
)

// this file will implement all the methods needed to implement the store interface

type MemoryStore struct {
	// we need one mutex and one map
	mu        sync.RWMutex
	data      map[string]string
	maxLength int
}

func NewMemoryStore(maxLength int) *MemoryStore {
	// made a constructor so that we can write extra logic here without changes at many places

	m := &MemoryStore{}
	m.data = make(map[string]string)
	m.maxLength = maxLength

	return m
}

func (m *MemoryStore) Get(key string) (string, error) {
	// lock the data with read
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.data[key]

	if !ok {
		// return error that key not present
		return "", fmt.Errorf("key %q not found", key)
	}

	return val, nil
}

func (m *MemoryStore) Set(key string, val string) error {

	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	if len(val) > m.maxLength {
		return fmt.Errorf("value cannot be greater than %d", m.maxLength)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = val

	return nil
}

func (m *MemoryStore) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

// Snapshot serializes a consistent whole-map image while holding the read
// lock. Raft treats these bytes as opaque state-machine data.
func (m *MemoryStore) Snapshot() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, err := json.Marshal(m.data)
	if err != nil {
		return nil, fmt.Errorf("encode memory store snapshot: %w", err)
	}
	return data, nil
}

// Restore replaces, rather than merges, the entire local read model. Decode
// and validate before acquiring the write lock so a malformed snapshot cannot
// leave a partially restored store behind.
func (m *MemoryStore) Restore(data []byte) error {
	decoded := make(map[string]string)
	if err := json.Unmarshal(data, &decoded); err != nil {
		return fmt.Errorf("decode memory store snapshot: %w", err)
	}
	for key, value := range decoded {
		if key == "" {
			return fmt.Errorf("snapshot contains an empty key")
		}
		if len(value) > m.maxLength {
			return fmt.Errorf("snapshot value for key %q exceeds maximum length %d", key, m.maxLength)
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = decoded
	return nil
}
