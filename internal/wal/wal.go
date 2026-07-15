package wal

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

// we make the wal struct

type WAL struct {
	// we need a file pointer a mutex and
	file *os.File
	mu   sync.Mutex
	path string
}

type Entry struct {
	Op    string // "SET" or "DELETE"
	Key   string
	Value string // empty for DELETE
}

// os.open for read only
func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file: file,
		path: path,
	}, nil
}

// more wal methods - append , replay ,close

func (w *WAL) Append(op, key, value string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Format:
	// timestamp<TAB>operation<TAB>key<TAB>value<NEWLINE>
	line := fmt.Sprintf(
		"%d\t%s\t%s\t%s\n",
		time.Now().Unix(),
		op,
		key,
		value,
	)

	// Write the entry.
	if _, err := w.file.WriteString(line); err != nil {
		return fmt.Errorf("failed to write to WAL %q: %w", w.path, err)
	}

	// Flush to disk for durability.
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL %q: %w", w.path, err)
	}

	return nil
}

func (w *WAL) Replay() ([]Entry, error) {
	// move the cursor to the start
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := w.file.Seek(0, io.SeekStart)

	if err != nil {
		return nil, fmt.Errorf("error moving cursor to start , %w", err)
	}
	scanner := bufio.NewScanner(w.file)

	var entries []Entry

	for scanner.Scan() {
		line := scanner.Text()
		// split the line on t create entries
		parts := strings.SplitN(line, "\t", 4)

		if len(parts) <3 {
			continue
		}

		e := Entry{
			Op:    parts[1],
			Key:   parts[2],
			Value: parts[3],
		}
		entries = append(entries, e)

	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return entries, nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}

	err := w.file.Close()
	w.file = nil

	return err
}
