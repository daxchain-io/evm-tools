package backoff

import (
	"context"
	"testing"
	"time"
)

func TestDuration(t *testing.T) {
	base, maxDelay := 500*time.Millisecond, 30*time.Second
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{-1, base},      // < 1 is treated as 1
		{0, base},       // < 1 is treated as 1
		{1, base},       // 500ms
		{2, 2 * base},   // 1s
		{3, 4 * base},   // 2s
		{6, 32 * base},  // 16s
		{7, maxDelay},   // would be 32s, capped at 30s
		{100, maxDelay}, // capped
	}
	for _, c := range cases {
		if got := Duration(c.attempt, base, maxDelay); got != c.want {
			t.Errorf("Duration(%d) = %v, want %v", c.attempt, got, c.want)
		}
	}
}

func TestJitter(t *testing.T) {
	if got := Jitter(0); got != 0 {
		t.Errorf("Jitter(0) = %v, want 0", got)
	}
	if got := Jitter(-time.Second); got != 0 {
		t.Errorf("Jitter(-1s) = %v, want 0", got)
	}
	d := 10 * time.Second
	for range 1000 {
		got := Jitter(d)
		if got < d/2 || got > d {
			t.Fatalf("Jitter(%v) = %v, outside [%v, %v]", d, got, d/2, d)
		}
	}
}

func TestSleep(t *testing.T) {
	if !Sleep(context.Background(), time.Millisecond) {
		t.Error("Sleep(1ms) = false, want true (slept the full duration)")
	}
	if !Sleep(context.Background(), 0) {
		t.Error("Sleep(0) = false, want true (immediate, ctx live)")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if Sleep(ctx, time.Hour) {
		t.Error("Sleep(cancelled, 1h) = true, want false")
	}
	if Sleep(ctx, 0) {
		t.Error("Sleep(cancelled, 0) = true, want false (ctx already done)")
	}
}
