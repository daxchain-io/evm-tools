package checkpoint

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSaveLoadRoundTrip(t *testing.T) {
	// Path in a not-yet-existing subdir to exercise MkdirAll.
	p := filepath.Join(t.TempDir(), "state", "cursor.json")
	s := NewStore(p)

	if _, ok, err := s.Load(); err != nil || ok {
		t.Fatalf("Load before save: ok=%v err=%v, want (false,nil)", ok, err)
	}

	want := Cursor{ChainID: 1, LastBlock: 12345}
	if err := s.Save(want); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, ok, err := s.Load()
	if err != nil || !ok {
		t.Fatalf("Load after save: ok=%v err=%v", ok, err)
	}
	if got != want {
		t.Errorf("round trip: got %+v, want %+v", got, want)
	}

	// Overwrite is atomic and leaves no temp file behind.
	if err := s.Save(Cursor{ChainID: 1, LastBlock: 12400}); err != nil {
		t.Fatalf("Save overwrite: %v", err)
	}
	if got, _, _ := s.Load(); got.LastBlock != 12400 {
		t.Errorf("overwrite: last_block = %d, want 12400", got.LastBlock)
	}
	if _, err := os.Stat(p + ".tmp"); !os.IsNotExist(err) {
		t.Errorf("temp file should not linger after Save")
	}
}

func TestLoadCorruptIsError(t *testing.T) {
	p := filepath.Join(t.TempDir(), "cursor.json")
	if err := os.WriteFile(p, []byte("{not json"), 0o640); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := NewStore(p).Load(); err == nil || ok {
		t.Errorf("corrupt cursor must surface an error, got ok=%v err=%v", ok, err)
	}
}
