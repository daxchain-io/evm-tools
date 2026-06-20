package filesink

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// dirPerm/filePerm are the modes for the output directory and files. The active
// file is 0o644 (owner-write, world-read) — JSONL records are not secrets, but
// callers can pre-create the directory with a tighter mode if needed.
const (
	dirPerm  os.FileMode = 0o755
	filePerm os.FileMode = 0o644
)

// rotatedTimeFormat stamps rotated segments with a sortable, millisecond-precise
// UTC timestamp so lexical order matches chronological order (used for pruning).
const rotatedTimeFormat = "20060102T150405.000"

// RotateConfig configures a rotating Writer.
type RotateConfig struct {
	// Path is the active output file (required).
	Path string
	// MaxSize rotates the active file once it reaches this many bytes. 0 disables
	// size-based rotation.
	MaxSize int64
	// MaxAge rotates the active file once it reaches this age (measured from when
	// the Writer opened it). 0 disables time-based rotation.
	MaxAge time.Duration
	// MaxBackups caps retained rotated segments (oldest pruned first). 0 keeps all.
	MaxBackups int
	// Compress gzips each rotated segment.
	Compress bool
	// Fsync flushes each write to stable storage before Write returns.
	Fsync bool
	// Logger receives best-effort warnings (compression/prune failures that do not
	// risk data). Defaults to slog.Default().
	Logger *slog.Logger
	// OnRotate, when set, is called after each successful rotation (for metrics).
	OnRotate func()
	// Now is injectable for tests; defaults to time.Now.
	Now func() time.Time
}

// file is the subset of *os.File the Writer drives. It is an interface so a test
// can inject a fault-injecting wrapper to exercise the partial-write rollback
// path (which a real *os.File will not produce on demand).
type file interface {
	io.Writer
	Truncate(size int64) error
	Sync() error
	Close() error
}

// Writer appends record lines (each followed by a newline, written in a single
// syscall so a line is never torn) to an active file, rotating it by size and/or
// age. Rotated segments are timestamped, optionally gzip-compressed, and pruned
// to MaxBackups. A Writer is NOT safe for concurrent use — the sink drives it
// from a single goroutine.
type Writer struct {
	cfg      RotateConfig
	log      *slog.Logger
	now      func() time.Time
	f        file
	size     int64
	openedAt time.Time
	buf      []byte // reused line+newline scratch so each line is one Write
}

// NewWriter opens (creating parent directories and the active file as needed) and
// returns a rotating Writer. An existing active file is appended to, not
// truncated, so a restart continues the same file.
func NewWriter(cfg RotateConfig) (*Writer, error) {
	if strings.TrimSpace(cfg.Path) == "" {
		return nil, errors.New("filesink: path is required")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	w := &Writer{cfg: cfg, log: cfg.Logger, now: cfg.Now}
	if err := w.open(); err != nil {
		return nil, err
	}
	return w, nil
}

// open ensures the parent directory exists, opens the active file for append, and
// records its current size and the open time (the age-rotation reference).
func (w *Writer) open() error {
	if dir := filepath.Dir(w.cfg.Path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, dirPerm); err != nil {
			return fmt.Errorf("filesink: create output directory: %w", err)
		}
	}
	f, err := os.OpenFile(w.cfg.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, filePerm)
	if err != nil {
		return fmt.Errorf("filesink: open %q: %w", w.cfg.Path, err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("filesink: stat %q: %w", w.cfg.Path, err)
	}
	w.f = f
	w.size = info.Size()
	w.openedAt = w.now()
	return nil
}

// Write appends line plus a trailing newline as one syscall, rotating first when
// the configured size/age thresholds are reached. It returns the number of bytes
// written (line + newline).
func (w *Writer) Write(line []byte) (int, error) {
	need := int64(len(line)) + 1
	if w.shouldRotate(need) {
		if err := w.rotate(); err != nil {
			return 0, err
		}
	}
	w.buf = append(w.buf[:0], line...)
	w.buf = append(w.buf, '\n')
	n, err := w.f.Write(w.buf)
	if err != nil {
		// A short write (n>0) can happen on a filling disk: the kernel writes part
		// of the buffer, then the next syscall returns ENOSPC. Because the file is
		// O_APPEND and the disk-full error is retried (Classify -> transient), a
		// full-payload retry would append the whole line AFTER the durable fragment
		// — tearing the line and duplicating bytes. Roll the fragment back to the
		// pre-write size so the retry re-appends the whole line cleanly and size
		// stays exact. If the rollback itself fails we can no longer guarantee a
		// clean append, so surface a (permanent) error and fail fast rather than
		// risk corrupting the stream.
		if n > 0 {
			if terr := w.f.Truncate(w.size); terr != nil {
				return n, fmt.Errorf("filesink: rollback of %d-byte partial write failed: %w (write error: %v)", n, terr, err)
			}
		}
		return n, err
	}
	w.size += int64(n)
	if w.cfg.Fsync {
		if err := w.f.Sync(); err != nil {
			return n, fmt.Errorf("filesink: fsync: %w", err)
		}
	}
	return n, nil
}

// shouldRotate reports whether the active file must rotate before writing `need`
// more bytes. An empty file never rotates (avoids zero-byte segments and an
// infinite rotate loop when a single line exceeds MaxSize).
func (w *Writer) shouldRotate(need int64) bool {
	if w.size == 0 {
		return false
	}
	if w.cfg.MaxSize > 0 && w.size+need > w.cfg.MaxSize {
		return true
	}
	if w.cfg.MaxAge > 0 && w.now().Sub(w.openedAt) >= w.cfg.MaxAge {
		return true
	}
	return false
}

// rotate closes the active file, renames it to a timestamped segment, optionally
// compresses it (best-effort — a compression failure keeps the uncompressed
// segment and never loses data), opens a fresh active file, and prunes old
// segments.
func (w *Writer) rotate() error {
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("filesink: close before rotate: %w", err)
	}
	rotated := w.rotatedName()
	if err := os.Rename(w.cfg.Path, rotated); err != nil {
		return fmt.Errorf("filesink: rename on rotate: %w", err)
	}
	if w.cfg.Compress {
		if _, err := compressFile(rotated); err != nil {
			// The uncompressed segment remains, so no data is lost; warn and move on.
			w.log.Warn("rotated segment compression failed; keeping uncompressed",
				"error", err.Error())
		}
	}
	if err := w.open(); err != nil {
		return err
	}
	w.prune()
	if w.cfg.OnRotate != nil {
		w.cfg.OnRotate()
	}
	return nil
}

// rotatedName builds a unique, sortable name for the segment being rotated out,
// e.g. events.jsonl -> events-20260620T010203.000.jsonl. A collision (two
// rotations within the same millisecond) gets a numeric suffix.
func (w *Writer) rotatedName() string {
	dir := filepath.Dir(w.cfg.Path)
	name := filepath.Base(w.cfg.Path)
	ext := filepath.Ext(name)
	base := strings.TrimSuffix(name, ext)
	ts := w.now().UTC().Format(rotatedTimeFormat)

	candidate := filepath.Join(dir, base+"-"+ts+ext)
	for i := 1; ; i++ {
		if !pathExists(candidate) && !pathExists(candidate+".gz") {
			return candidate
		}
		candidate = filepath.Join(dir, fmt.Sprintf("%s-%s-%d%s", base, ts, i, ext))
	}
}

// prune removes the oldest rotated segments beyond MaxBackups. It matches both
// uncompressed (.jsonl) and compressed (.jsonl.gz) segments sharing the active
// file's base name, sorted lexically (== chronologically, by the timestamp).
func (w *Writer) prune() {
	if w.cfg.MaxBackups <= 0 {
		return
	}
	dir := filepath.Dir(w.cfg.Path)
	name := filepath.Base(w.cfg.Path)
	ext := filepath.Ext(name)
	base := strings.TrimSuffix(name, ext)
	prefix := base + "-"

	entries, err := os.ReadDir(dir)
	if err != nil {
		w.log.Warn("prune: read output directory failed", "error", err.Error())
		return
	}
	type segment struct {
		name string
		mod  time.Time
	}
	var segments []segment
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		n := e.Name()
		if n == name {
			continue // the active file
		}
		if !strings.HasPrefix(n, prefix) {
			continue
		}
		if strings.HasSuffix(n, ext) || strings.HasSuffix(n, ext+".gz") {
			mod := time.Time{}
			if info, ierr := e.Info(); ierr == nil {
				mod = info.ModTime()
			}
			segments = append(segments, segment{name: n, mod: mod})
		}
	}
	if len(segments) <= w.cfg.MaxBackups {
		return
	}
	// Order oldest-first by modification time (robust to same-timestamp segment
	// names); the sortable name breaks ties. Prune everything past MaxBackups.
	sort.Slice(segments, func(i, j int) bool {
		if segments[i].mod.Equal(segments[j].mod) {
			return segments[i].name < segments[j].name
		}
		return segments[i].mod.Before(segments[j].mod)
	})
	for _, old := range segments[:len(segments)-w.cfg.MaxBackups] {
		if err := os.Remove(filepath.Join(dir, old.name)); err != nil {
			w.log.Warn("prune: remove old segment failed", "error", err.Error())
		}
	}
}

// Sync flushes the active file to stable storage.
func (w *Writer) Sync() error {
	if w.f == nil {
		return nil
	}
	return w.f.Sync()
}

// Size returns the active file's current size in bytes.
func (w *Writer) Size() int64 { return w.size }

// Close flushes and closes the active file.
func (w *Writer) Close() error {
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

// compressFile gzips src to src+".gz" via a temp file renamed into place, then
// removes src. It returns the .gz path. On any failure the temp file is removed
// and src is left intact, so the segment is never lost.
func compressFile(src string) (string, error) {
	in, err := os.Open(src)
	if err != nil {
		return "", err
	}
	defer func() { _ = in.Close() }()

	dst := src + ".gz"
	tmp := dst + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, filePerm)
	if err != nil {
		return "", err
	}
	gz := gzip.NewWriter(out)
	if _, err := io.Copy(gz, in); err != nil {
		_ = gz.Close()
		_ = out.Close()
		_ = os.Remove(tmp)
		return "", err
	}
	if err := gz.Close(); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		return "", err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		return "", err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return "", err
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return "", err
	}
	if err := os.Remove(src); err != nil {
		// The .gz is durable; a leftover source is harmless but worth reporting.
		return dst, err
	}
	return dst, nil
}

func pathExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}
