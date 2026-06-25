// Package backoff provides the shared exponential-backoff retry policy used by the
// producers, the record transport, and every sink: base*2^(attempt-1) capped at
// max, with [d/2, d] jitter, and a context-aware sleep.
package backoff

import (
	"context"
	"math/rand"
	"time"
)

// Duration returns the backoff for a 1-based attempt: base doubled per attempt,
// capped at maxDelay. attempt < 1 is treated as 1.
func Duration(attempt int, base, maxDelay time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := base
	for i := 1; i < attempt && d < maxDelay; i++ {
		d *= 2
	}
	if d > maxDelay {
		d = maxDelay
	}
	return d
}

// Jitter returns a duration in [d/2, d], or 0 when d <= 0.
func Jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	half := d / 2
	return half + time.Duration(rand.Int63n(int64(half)+1))
}

// Sleep blocks for d or until ctx is done, returning true if it slept the full
// duration and false if ctx was cancelled. d <= 0 returns immediately (true unless
// ctx is already done).
func Sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
