package redissink

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/daxchain-io/evm-tools/internal/record"
)

// defaultDedupPrefix namespaces the per-record dedup marker keys so they do not
// collide with application keys in a shared Redis.
const defaultDedupPrefix = "evmtools:dedup:"

// ClientConfig configures the real go-redis-backed appender. URL is a secret (it
// may carry a password) and is never logged; Target() exposes only host/port/db.
type ClientConfig struct {
	URL         string
	Stream      string
	Field       string
	MaxLen      int64
	Dedup       bool
	DedupTTL    time.Duration
	DedupPrefix string
}

// dedupScript appends a record to the stream only if its dedup marker is absent,
// then sets the marker. Idempotency comes from whole-script atomicity plus the
// EXISTS gate: Redis runs the script atomically and never persists partial in-script
// effects, so a retry either finds the marker (already delivered -> dedup, return 0)
// or finds nothing (re-append). The XADD-before-SET ordering matters only for the
// client-died-after-completion retry: the server applied both writes but the client
// missed the reply, and the present marker makes the retry a no-op. (A mid-script
// server crash loses BOTH writes regardless of order, and the retry safely
// re-appends via the still-absent marker.) Returns 1 when a new entry was added,
// 0 when deduplicated.
//
//	KEYS[1] = stream key      KEYS[2] = dedup marker key
//	ARGV[1] = field name      ARGV[2] = payload
//	ARGV[3] = maxlen (0=none) ARGV[4] = marker ttl seconds (0=none)
const dedupScript = `
if redis.call('EXISTS', KEYS[2]) == 1 then
  return 0
end
local maxlen = tonumber(ARGV[3])
if maxlen > 0 then
  redis.call('XADD', KEYS[1], 'MAXLEN', '~', maxlen, '*', ARGV[1], ARGV[2])
else
  redis.call('XADD', KEYS[1], '*', ARGV[1], ARGV[2])
end
local ttl = tonumber(ARGV[4])
if ttl > 0 then
  redis.call('SET', KEYS[2], '1', 'EX', ttl)
else
  redis.call('SET', KEYS[2], '1')
end
return 1
`

type redisAppender struct {
	client *redis.Client
	cfg    ClientConfig
	target string
	script *redis.Script
}

// NewAppender opens a Redis client from cfg.URL and returns a ready Appender. The
// URL is a secret and is never logged; a parse failure returns a generic error
// (no URL echoed) so the password cannot leak.
func NewAppender(cfg ClientConfig) (Appender, error) {
	if strings.TrimSpace(cfg.Stream) == "" {
		return nil, errors.New("redissink: stream key is required")
	}
	opt, err := redis.ParseURL(cfg.URL)
	if err != nil {
		// Do not wrap err: go-redis may echo the URL (with password) in it.
		return nil, errors.New("redissink: invalid redis url (expected redis:// or rediss://; check [redis].url)")
	}
	if cfg.Field == "" {
		cfg.Field = "data"
	}
	if cfg.DedupPrefix == "" {
		cfg.DedupPrefix = defaultDedupPrefix
	}
	a := &redisAppender{
		client: redis.NewClient(opt),
		cfg:    cfg,
		target: redactTarget(opt),
	}
	if cfg.Dedup {
		a.script = redis.NewScript(dedupScript)
	}
	return a, nil
}

func (a *redisAppender) Append(ctx context.Context, env record.Envelope, raw []byte) (bool, error) {
	if !a.cfg.Dedup {
		args := &redis.XAddArgs{
			Stream: a.cfg.Stream,
			Values: map[string]any{a.cfg.Field: raw},
		}
		if a.cfg.MaxLen > 0 {
			args.MaxLen = a.cfg.MaxLen
			args.Approx = true
		}
		if err := a.client.XAdd(ctx, args).Err(); err != nil {
			return false, err
		}
		return true, nil
	}

	ttlSec := int64(0)
	if a.cfg.DedupTTL > 0 {
		// Redis SET EX is whole-second granularity. Round UP so a fractional TTL is
		// never silently shortened (e.g. 1500ms -> 2s, not 1s) and a sub-second TTL
		// still yields at least 1s rather than expiring immediately.
		ttlSec = int64((a.cfg.DedupTTL + time.Second - 1) / time.Second)
	}
	markerKey := a.cfg.DedupPrefix + env.DedupKey()
	res, err := a.script.Run(ctx, a.client,
		[]string{a.cfg.Stream, markerKey},
		a.cfg.Field, raw, a.cfg.MaxLen, ttlSec,
	).Int64()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

func (a *redisAppender) Reachable(ctx context.Context) error { return a.client.Ping(ctx).Err() }
func (a *redisAppender) Target() string                      { return a.target }
func (a *redisAppender) Close() error                        { return a.client.Close() }

// redactTarget builds a log-safe destination string from the parsed options:
// host/port and database index only — never the username or password.
func redactTarget(opt *redis.Options) string {
	scheme := "redis"
	if opt.TLSConfig != nil {
		scheme = "rediss"
	}
	return fmt.Sprintf("%s://%s/%d", scheme, opt.Addr, opt.DB)
}
