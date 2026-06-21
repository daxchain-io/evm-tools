// Package checkpoint provides a small, durable resume cursor for evm-stream: the
// highest block whose records the producer has finished emitting. On restart the
// producer resumes from cursor+1 instead of jumping to the chain head, so no
// blocks are missed across a restart. Re-emitting the boundary block on resume is
// harmless — sinks dedup via the record's reorg-stable dedup key — so the contract
// stays at-least-once and gap-free.
//
// The cursor is a tiny JSON file written atomically (temp file + fsync + rename)
// so a crash mid-write never leaves a torn or empty cursor.
package checkpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// dirPerm/filePerm mirror the suite's file-sink conventions: a private-ish cursor
// (it carries no secret, only a chain id and block height, but there is no reason
// to make it world-readable).
const (
	dirPerm  os.FileMode = 0o750
	filePerm os.FileMode = 0o640
)

// Cursor is the persisted resume position: the chain it belongs to and the
// highest fully-processed block. ChainID guards against resuming a cursor written
// for a different chain (e.g. a reused path after a config change).
type Cursor struct {
	ChainID   int64  `json:"chain_id"`
	LastBlock uint64 `json:"last_block"`
}

// Store reads and writes a Cursor at a fixed path.
type Store struct {
	path string
}

// NewStore returns a Store backed by path.
func NewStore(path string) *Store { return &Store{path: path} }

// Path returns the cursor file path (for logging).
func (s *Store) Path() string { return s.path }

// Load returns the persisted cursor. ok is false (with a nil error) when the
// file does not exist yet — the first run. A present-but-unparseable file is a
// real error so a corrupt cursor surfaces rather than being silently treated as
// "start fresh".
func (s *Store) Load() (Cursor, bool, error) {
	b, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return Cursor{}, false, nil
	}
	if err != nil {
		return Cursor{}, false, fmt.Errorf("checkpoint: read %s: %w", s.path, err)
	}
	var c Cursor
	if err := json.Unmarshal(b, &c); err != nil {
		return Cursor{}, false, fmt.Errorf("checkpoint: parse %s: %w", s.path, err)
	}
	return c, true, nil
}

// Save atomically persists the cursor: it writes a sibling temp file, fsyncs it,
// and renames it over the target so a reader never observes a partial write and a
// crash mid-save leaves either the old cursor or the new one — never a torn one.
func (s *Store) Save(c Cursor) error {
	if err := os.MkdirAll(filepath.Dir(s.path), dirPerm); err != nil {
		return fmt.Errorf("checkpoint: create dir: %w", err)
	}
	b, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("checkpoint: marshal: %w", err)
	}
	b = append(b, '\n')

	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, filePerm)
	if err != nil {
		return fmt.Errorf("checkpoint: open temp: %w", err)
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("checkpoint: write temp: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("checkpoint: fsync temp: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("checkpoint: close temp: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("checkpoint: rename: %w", err)
	}
	return nil
}
