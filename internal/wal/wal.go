// Package wal provides a small durable append-only record file.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const maxRecordSize = 16 << 20 // 16 MiB: protects recovery from corrupt sizes.

type WAL struct {
	file *os.File
	mu   sync.Mutex
	path string
}

func NewWAL(path string) (*WAL, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("create WAL directory for %q: %w", path, err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open WAL %q: %w", path, err)
	}
	return &WAL{file: file, path: path}, nil
}

// Append durably writes one length-prefixed record. A record is visible to a
// caller only after both its bytes and the file metadata have been synced.
func (w *WAL) Append(record []byte) error {
	if len(record) == 0 || len(record) > maxRecordSize {
		return fmt.Errorf("WAL record size %d is invalid", len(record))
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return errors.New("append to closed WAL")
	}

	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(record)))
	if _, err := w.file.Write(header[:]); err != nil {
		return fmt.Errorf("write WAL header %q: %w", w.path, err)
	}
	if _, err := w.file.Write(record); err != nil {
		return fmt.Errorf("write WAL record %q: %w", w.path, err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync WAL %q: %w", w.path, err)
	}
	return nil
}

// Replay returns complete records in order. A crash can leave a partial final
// frame; it is discarded and truncated before the WAL accepts another append.
func (w *WAL) Replay() ([][]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil, errors.New("replay closed WAL")
	}

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek WAL %q: %w", w.path, err)
	}

	var records [][]byte
	var validBytes int64
	for {
		var header [4]byte
		_, err := io.ReadFull(w.file, header[:])
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			if err := w.truncateLocked(validBytes); err != nil {
				return nil, err
			}
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read WAL header %q: %w", w.path, err)
		}

		size := int(binary.BigEndian.Uint32(header[:]))
		if size == 0 || size > maxRecordSize {
			return nil, fmt.Errorf("WAL %q has invalid record size %d at byte %d", w.path, size, validBytes)
		}
		record := make([]byte, size)
		if _, err := io.ReadFull(w.file, record); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				if truncateErr := w.truncateLocked(validBytes); truncateErr != nil {
					return nil, truncateErr
				}
				break
			}
			return nil, fmt.Errorf("read WAL record %q: %w", w.path, err)
		}
		records = append(records, record)
		validBytes += int64(len(header) + size)
	}

	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("seek WAL end %q: %w", w.path, err)
	}
	return records, nil
}

func (w *WAL) truncateLocked(size int64) error {
	if err := w.file.Truncate(size); err != nil {
		return fmt.Errorf("truncate torn WAL record %q: %w", w.path, err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync truncated WAL %q: %w", w.path, err)
	}
	return nil
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
