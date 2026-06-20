package filesink

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"
)

// fakeClock is an injectable, advanceable clock for age-rotation tests.
type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 6, 20, 1, 2, 3, 0, time.UTC)}
}

// readLines returns the newline-delimited lines of a file (no trailing empty).
func readLines(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	s := strings.TrimRight(string(b), "\n")
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

// rotatedSegments lists rotated segment names (anything matching base-* that is
// not the active file) sorted ascending.
func rotatedSegments(t *testing.T, dir, active string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	var out []string
	for _, e := range entries {
		if e.Name() == active || e.IsDir() {
			continue
		}
		out = append(out, e.Name())
	}
	return out
}

func TestWriterAppendsLinesAndTracksSize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	w, err := NewWriter(RotateConfig{Path: path})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, line := range []string{"a", "bb", "ccc"} {
		if _, err := w.Write([]byte(line)); err != nil {
			t.Fatalf("Write(%q): %v", line, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := readLines(t, path)
	want := []string{"a", "bb", "ccc"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("lines = %v, want %v", got, want)
	}
	// "a\n" + "bb\n" + "ccc\n" = 2 + 3 + 4 = 9 bytes.
	if w.Size() != 9 {
		t.Errorf("Size = %d, want 9", w.Size())
	}
}

func TestWriterResumeAppends(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")

	w1, err := NewWriter(RotateConfig{Path: path})
	if err != nil {
		t.Fatalf("NewWriter 1: %v", err)
	}
	_, _ = w1.Write([]byte("first"))
	_ = w1.Close()

	w2, err := NewWriter(RotateConfig{Path: path})
	if err != nil {
		t.Fatalf("NewWriter 2: %v", err)
	}
	if w2.Size() != int64(len("first")+1) {
		t.Errorf("resumed Size = %d, want %d", w2.Size(), len("first")+1)
	}
	_, _ = w2.Write([]byte("second"))
	_ = w2.Close()

	got := readLines(t, path)
	if len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Errorf("resumed file lines = %v, want [first second]", got)
	}
}

func TestWriterRotateBySize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	clk := newClock()
	// MaxSize 5 bytes: "aaaa\n" = 5 bytes fills it; the next write rotates.
	w, err := NewWriter(RotateConfig{Path: path, MaxSize: 5, Now: clk.Now})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Write([]byte("aaaa")); err != nil { // size -> 5
		t.Fatalf("write 1: %v", err)
	}
	clk.advance(time.Second)                           // distinct rotation timestamp
	if _, err := w.Write([]byte("bbbb")); err != nil { // size+need > 5 -> rotate first
		t.Fatalf("write 2: %v", err)
	}
	_ = w.Close()

	// Active file holds the most recent line.
	active := readLines(t, path)
	if len(active) != 1 || active[0] != "bbbb" {
		t.Errorf("active lines = %v, want [bbbb]", active)
	}
	// Exactly one rotated segment holding the first line.
	segs := rotatedSegments(t, dir, "events.jsonl")
	if len(segs) != 1 {
		t.Fatalf("rotated segments = %v, want exactly 1", segs)
	}
	rotated := readLines(t, filepath.Join(dir, segs[0]))
	if len(rotated) != 1 || rotated[0] != "aaaa" {
		t.Errorf("rotated lines = %v, want [aaaa]", rotated)
	}
}

func TestWriterRotateByAge(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	clk := newClock()
	w, err := NewWriter(RotateConfig{Path: path, MaxAge: time.Minute, Now: clk.Now})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Write([]byte("old")); err != nil {
		t.Fatalf("write 1: %v", err)
	}
	clk.advance(2 * time.Minute) // exceed MaxAge
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatalf("write 2: %v", err)
	}
	_ = w.Close()

	if active := readLines(t, path); len(active) != 1 || active[0] != "new" {
		t.Errorf("active lines = %v, want [new]", active)
	}
	if segs := rotatedSegments(t, dir, "events.jsonl"); len(segs) != 1 {
		t.Errorf("rotated segments = %v, want 1", segs)
	}
}

func TestWriterCompressRotatedSegment(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	clk := newClock()
	w, err := NewWriter(RotateConfig{Path: path, MaxSize: 5, Compress: true, Now: clk.Now})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	_, _ = w.Write([]byte("aaaa")) // size -> 5
	clk.advance(time.Second)
	_, _ = w.Write([]byte("bbbb")) // rotate
	_ = w.Close()

	segs := rotatedSegments(t, dir, "events.jsonl")
	if len(segs) != 1 {
		t.Fatalf("rotated segments = %v, want 1", segs)
	}
	seg := segs[0]
	if !strings.HasSuffix(seg, ".gz") {
		t.Errorf("rotated segment %q should be gzip-compressed (.gz)", seg)
	}
	// The uncompressed original must be gone.
	if pathExists(filepath.Join(dir, strings.TrimSuffix(seg, ".gz"))) {
		t.Errorf("uncompressed segment should be removed after compression")
	}
	// And it decompresses to the rotated line.
	f, err := os.Open(filepath.Join(dir, seg))
	if err != nil {
		t.Fatalf("open gz: %v", err)
	}
	defer func() { _ = f.Close() }()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	sc := bufio.NewScanner(gz)
	var lines []string
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	if len(lines) != 1 || lines[0] != "aaaa" {
		t.Errorf("decompressed lines = %v, want [aaaa]", lines)
	}
}

func TestWriterMaxBackupsPruneKeepsNewest(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	clk := newClock()
	// Each "rN\n" is 3 bytes (fills MaxSize=3), so each subsequent write rotates the
	// previous record into its own segment. Records r0..r4 produce segments holding
	// r0..r3 (oldest..newest); the active file holds r4. With MaxBackups=2 only the
	// two NEWEST rotated records (r2, r3) must survive — never the pruned r0/r1.
	w, err := NewWriter(RotateConfig{Path: path, MaxSize: 3, MaxBackups: 2, Now: clk.Now})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for i := range 5 {
		clk.advance(time.Second) // unique, increasing timestamps
		if _, err := w.Write(fmt.Appendf(nil, "r%d", i)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	_ = w.Close()

	segs := rotatedSegments(t, dir, "events.jsonl")
	if len(segs) != 2 {
		t.Fatalf("rotated segments after prune = %v, want 2", segs)
	}
	var content []string
	for _, s := range segs {
		content = append(content, readLines(t, filepath.Join(dir, s))...)
	}
	sort.Strings(content)
	if strings.Join(content, ",") != "r2,r3" {
		t.Errorf("surviving segment content = %v, want [r2 r3] (the two newest)", content)
	}
}

// shortWriteFile wraps a real file but tears the first `remaining` writes: it
// durably writes only a `prefix`-byte fragment and returns ENOSPC, exactly like a
// disk filling mid-write. After that it passes through. Truncate/Sync/Close pass
// through to the embedded *os.File.
type shortWriteFile struct {
	*os.File
	remaining int
	prefix    int
}

func (s *shortWriteFile) Write(p []byte) (int, error) {
	if s.remaining > 0 {
		s.remaining--
		k := min(s.prefix, len(p))
		n, _ := s.File.Write(p[:k]) // durable fragment, like a partial kernel write
		return n, syscall.ENOSPC
	}
	return s.File.Write(p)
}

func TestWriterPartialWriteRollsBackNoTornLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	w, err := NewWriter(RotateConfig{Path: path})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	// Inject a torn first write (2 durable bytes, then ENOSPC), then pass-through.
	realFile := w.f.(*os.File)
	w.f = &shortWriteFile{File: realFile, remaining: 1, prefix: 2}

	// The torn write surfaces ENOSPC (transient) and rolls the fragment back.
	if _, err := w.Write([]byte("hello")); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("want ENOSPC from torn write, got %v", err)
	}
	if w.Size() != 0 {
		t.Errorf("size after rollback = %d, want 0 (fragment rolled back)", w.Size())
	}
	// The retry (fault cleared) appends the whole line cleanly.
	if _, err := w.Write([]byte("hello")); err != nil {
		t.Fatalf("retry write: %v", err)
	}
	_ = w.Close()
	got := readLines(t, path)
	if len(got) != 1 || got[0] != "hello" {
		t.Errorf("file = %v, want exactly [hello] (no torn fragment, no duplicate)", got)
	}
}

func TestWriterFsyncWritesToDisk(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	w, err := NewWriter(RotateConfig{Path: path, Fsync: true})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Write([]byte("durable")); err != nil {
		t.Fatalf("fsync write: %v", err)
	}
	_ = w.Close()
	if got := readLines(t, path); len(got) != 1 || got[0] != "durable" {
		t.Errorf("fsync file = %v, want [durable]", got)
	}
}

func TestWriterCompressFailureKeepsUncompressed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	clk := newClock()
	w, err := NewWriter(RotateConfig{Path: path, MaxSize: 5, Compress: true, Now: clk.Now})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	_, _ = w.Write([]byte("aaaa")) // size -> 5
	clk.advance(time.Second)
	// Block compression: pre-create a DIRECTORY at the compressor's temp path so
	// its OpenFile fails. Rotation must still complete and keep the uncompressed
	// segment (best-effort compression, no data loss).
	ts := clk.t.UTC().Format(rotatedTimeFormat)
	rotated := filepath.Join(dir, "events-"+ts+".jsonl")
	if err := os.Mkdir(rotated+".gz.tmp", 0o755); err != nil {
		t.Fatalf("mkdir temp blocker: %v", err)
	}
	_, _ = w.Write([]byte("bbbb")) // rotate; compression fails best-effort
	_ = w.Close()

	if !pathExists(rotated) {
		t.Errorf("uncompressed segment %s should remain when compression fails", rotated)
	}
	if pathExists(rotated + ".gz") {
		t.Errorf("no .gz should exist when compression failed")
	}
	if got := readLines(t, rotated); len(got) != 1 || got[0] != "aaaa" {
		t.Errorf("rotated segment content = %v, want [aaaa] (no data loss)", got)
	}
}

func TestWriterEmptyFileNeverRotatesAndOversizeLineFits(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	clk := newClock()
	w, err := NewWriter(RotateConfig{Path: path, MaxSize: 2, Now: clk.Now})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	// A single line larger than MaxSize must still be written to the empty active
	// file (no zero-byte segment, no infinite rotate loop).
	if _, err := w.Write([]byte("hugeline")); err != nil {
		t.Fatalf("write oversize: %v", err)
	}
	if segs := rotatedSegments(t, dir, "events.jsonl"); len(segs) != 0 {
		t.Errorf("no rotation expected for first write, got segments %v", segs)
	}
	clk.advance(time.Second)
	// The next write rotates because the file is now non-empty and over size.
	if _, err := w.Write([]byte("next")); err != nil {
		t.Fatalf("write 2: %v", err)
	}
	_ = w.Close()
	if segs := rotatedSegments(t, dir, "events.jsonl"); len(segs) != 1 {
		t.Errorf("expected 1 rotated segment, got %v", segs)
	}
	if active := readLines(t, path); len(active) != 1 || active[0] != "next" {
		t.Errorf("active = %v, want [next]", active)
	}
}

func TestWriterCreatesParentDirectory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "deeper", "events.jsonl")
	w, err := NewWriter(RotateConfig{Path: path})
	if err != nil {
		t.Fatalf("NewWriter should create parent dirs: %v", err)
	}
	_, _ = w.Write([]byte("x"))
	_ = w.Close()
	if !pathExists(path) {
		t.Errorf("expected file created at %s", path)
	}
}
