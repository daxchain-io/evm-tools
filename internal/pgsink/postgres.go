package pgsink

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daxchain-io/evm-tools/internal/record"
)

// tableNameRe matches a safe SQL identifier, optionally schema-qualified.
var tableNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$`)

// ValidateTableName ensures table is a safe identifier. The table name is
// interpolated into DDL/INSERT (it cannot be a bind parameter), so it is validated
// strictly — letters/digits/underscore with an optional single "schema." prefix —
// to keep it injection-safe.
func ValidateTableName(table string) error {
	if !tableNameRe.MatchString(table) {
		return fmt.Errorf("pgsink: invalid table name %q (allowed: letters, digits, underscore; optional schema.table)", table)
	}
	return nil
}

type pgInserter struct {
	pool      *pgxpool.Pool
	insertSQL string
	target    string
}

// NewInserter opens a pgx connection pool from dsn, optionally creates the target
// table, and returns a ready Inserter. dsn carries the password and is never
// logged; Target() returns a redacted host/port/db form.
func NewInserter(ctx context.Context, dsn, table string, createTable bool) (Inserter, error) {
	if err := ValidateTableName(table); err != nil {
		return nil, err
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		// pgx's parse error names the host/database, not the password.
		return nil, fmt.Errorf("pgsink: parse dsn: %w", err)
	}
	target := fmt.Sprintf("postgres://%s:%d/%s", cfg.ConnConfig.Host, cfg.ConnConfig.Port, cfg.ConnConfig.Database)

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pgsink: connect to %s: %w", target, err)
	}
	if createTable {
		if _, derr := pool.Exec(ctx, createTableSQL(table)); derr != nil {
			pool.Close()
			return nil, fmt.Errorf("pgsink: create table %s: %w", table, derr)
		}
	}

	// Validate the insert (including the ON CONFLICT (dedup_key) target) against
	// the actual table at startup by preparing it: a missing table/column or a
	// missing UNIQUE index / PRIMARY KEY on dedup_key (which ON CONFLICT requires)
	// fails here with a clear, actionable error rather than mid-stream on the first
	// record. This also forces a real connection now, so an unreachable or
	// misconfigured database fails fast at boot — a supervisor restart retries —
	// instead of after records have started flowing.
	stmt := insertSQL(table)
	conn, aerr := pool.Acquire(ctx)
	if aerr != nil {
		pool.Close()
		return nil, fmt.Errorf("pgsink: connect to %s: %w", target, aerr)
	}
	_, perr := conn.Conn().Prepare(ctx, "evm_tools_insert_validate", stmt)
	conn.Release()
	if perr != nil {
		pool.Close()
		return nil, fmt.Errorf("pgsink: target table %s must exist with the expected columns and a UNIQUE index or PRIMARY KEY on dedup_key (set create_table=true to create it): %w", table, perr)
	}
	return &pgInserter{pool: pool, insertSQL: stmt, target: target}, nil
}

func (p *pgInserter) Insert(ctx context.Context, env record.Envelope, raw []byte) error {
	_, err := p.pool.Exec(ctx, p.insertSQL,
		env.DedupKey(),             // $1  dedup_key (PK)
		env.SchemaVersion,          // $2  schema_version
		string(env.Type),           // $3  type
		env.Tool,                   // $4  tool
		nullText(env.Name),         // $5  name
		nullText(env.Chain),        // $6  chain
		env.ChainID,                // $7  chain_id
		int64(env.BlockNumber),     // $8  block_number
		nullText(env.BlockHash),    // $9  block_hash
		nullText(env.TxHash),       // $10 tx_hash
		nullLogIndex(env.LogIndex), // $11 log_index
		nullText(env.Timestamp),    // $12 timestamp (::timestamptz)
		env.EmittedAt,              // $13 emitted_at (::timestamptz)
		string(raw),                // $14 data (::jsonb) — verbatim record
	)
	return err
}

func (p *pgInserter) Reachable(ctx context.Context) error { return p.pool.Ping(ctx) }
func (p *pgInserter) Target() string                      { return p.target }
func (p *pgInserter) Close() error                        { p.pool.Close(); return nil }

// nullText maps an empty string to a SQL NULL so absent optional fields are NULL
// rather than empty strings.
func nullText(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// nullLogIndex maps a nil log index (non-log records) to SQL NULL.
func nullLogIndex(p *uint64) any {
	if p == nil {
		return nil
	}
	return int64(*p)
}

// quoteIdent double-quotes each part of an already-validated identifier.
func quoteIdent(table string) string {
	parts := strings.Split(table, ".")
	for i, p := range parts {
		parts[i] = `"` + p + `"`
	}
	return strings.Join(parts, ".")
}

func createTableSQL(table string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	dedup_key      text PRIMARY KEY,
	schema_version integer NOT NULL,
	type           text NOT NULL,
	tool           text NOT NULL,
	name           text,
	chain          text,
	chain_id       bigint,
	block_number   bigint,
	block_hash     text,
	tx_hash        text,
	log_index      bigint,
	timestamp      timestamptz,
	emitted_at     timestamptz NOT NULL,
	data           jsonb NOT NULL
)`, quoteIdent(table))
}

func insertSQL(table string) string {
	return fmt.Sprintf(`INSERT INTO %s
	(dedup_key, schema_version, type, tool, name, chain, chain_id, block_number, block_hash, tx_hash, log_index, "timestamp", emitted_at, data)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::timestamptz,$13::timestamptz,$14::jsonb)
	ON CONFLICT (dedup_key) DO NOTHING`, quoteIdent(table))
}
