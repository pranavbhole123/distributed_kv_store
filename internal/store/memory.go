package store

import (
	
	"fmt"
	"sync"
)

// this file will implement all the methods needed to implement the store interface

type MemoryStore struct {
	// we need one mutex and one map
	mu   sync.RWMutex
	data map[string]string
	maxLength int
}


func NewMemoryStore(maxLength int)(*MemoryStore){
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
	if len(val)>m.maxLength{
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
    _, ok := m.data[key]
    if !ok {
        return fmt.Errorf("key %q not found", key)
    }
    delete(m.data, key)
    return nil
}
