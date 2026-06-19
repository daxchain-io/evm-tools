// Package record defines the versioned JSONL contract shared by every tool in
// the suite and the single synchronized writer that emits it.
//
// This package is the source of truth for the data contract: producers
// construct records through it and any sink that parses records may depend on
// it directly. See docs/design.md ("Record Contract") for the normative
// specification.
//
// Output discipline (see docs/design.md, "Output discipline and backpressure"):
// every record is written through one [Writer], which serializes concurrent
// monitors onto a single stdout, writes each JSON object plus its trailing
// newline as one atomic operation, and flushes after every line. Records must
// never be written to stdout directly from a monitor goroutine.
package record

import "time"

// SchemaVersion is the current contract version. It starts at 1 and is bumped
// only on a breaking change to the envelope or an existing type's payload;
// additive changes (new optional fields, new record types) do not bump it.
const SchemaVersion = 1

// Type is the top-level record discriminator. It selects the shape of the
// envelope's Data payload.
type Type string

// Record type discriminators.
const (
	TypeEvent           Type = "event"
	TypeNativeTransfer  Type = "native_transfer"
	TypeBalanceSample   Type = "balance_sample"
	TypeBalanceChange   Type = "balance_change"
	TypeOwnershipSample Type = "ownership_sample"
	TypeOwnershipChange Type = "ownership_change"
	TypeContractSample  Type = "contract_sample"
	TypeContractChange  Type = "contract_change"
)

// Tool names that produce records.
const (
	ToolStream  = "evm-stream"
	ToolBalance = "evm-balance"
)

// Kind is the secondary discriminator tagging the asset class on balance_* and
// ownership_* records.
type Kind string

// Asset kinds.
const (
	KindNative Kind = "native"
	KindERC20  Kind = "erc20"
	KindERC721 Kind = "erc721"
)

// Field is the secondary discriminator tagging which contract observation a
// contract_* record carries.
type Field string

// Contract observation fields.
const (
	FieldNativeBalance    Field = "native_balance"
	FieldTokenTotalSupply Field = "token_total_supply"
	FieldTransferCount    Field = "transfer_count"
)

// Envelope is the common wrapper around every record. Fields that do not apply
// to a record type are omitted rather than emitted as null.
//
// Numeric encoding (see docs/design.md, "Numeric encoding"): only the small
// bounded integers below are JSON numbers — SchemaVersion, ChainID,
// BlockNumber, and LogIndex. Every 256-bit or token-precision amount lives in
// Data and is a JSON string.
type Envelope struct {
	SchemaVersion int    `json:"schema_version"`
	Type          Type   `json:"type"`
	Tool          string `json:"tool"`
	Name          string `json:"name"`
	Chain         string `json:"chain"`
	ChainID       int64  `json:"chain_id"`
	BlockNumber   uint64 `json:"block_number"`
	BlockHash     string `json:"block_hash,omitempty"`
	TxHash        string `json:"tx_hash,omitempty"`
	// LogIndex applies to event records. It is a pointer so index 0 is
	// preserved and non-log records omit the field entirely.
	LogIndex  *uint64 `json:"log_index,omitempty"`
	Timestamp string  `json:"timestamp,omitempty"`
	EmittedAt string  `json:"emitted_at"`
	Data      any     `json:"data"`
}

// RFC3339 renders a time as the contract's timestamp format (RFC 3339 in UTC).
// Both Timestamp and EmittedAt use this representation.
func RFC3339(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

// EventData is the payload for an "event" record: a decoded contract log.
type EventData struct {
	Event     string            `json:"event"`
	Signature string            `json:"signature"`
	Contract  string            `json:"contract"`
	Params    map[string]string `json:"params"`
}

// NativeTransferData is the payload for a "native_transfer" record: a
// successful top-level ETH transfer.
type NativeTransferData struct {
	From string `json:"from"`
	// To is omitted for contract-creation transactions.
	To       string `json:"to,omitempty"`
	ValueWei string `json:"value_wei"`
	Value    string `json:"value"`
	// ContractCreation is true when the transaction created a contract (To is
	// null). Omitted otherwise.
	ContractCreation bool `json:"contract_creation,omitempty"`
}

// BalanceData is the payload for "balance_sample" / "balance_change" records.
// The populated subset depends on Kind:
//
//   - native: BalanceWei, Balance.
//   - erc20:  Token, BalanceRaw, Balance, Decimals.
//   - erc721: Token, Count.
//
// Change records additionally set the matching Previous* field.
type BalanceData struct {
	Kind    Kind   `json:"kind"`
	Address string `json:"address"`
	Token   string `json:"token,omitempty"`

	BalanceWei string `json:"balance_wei,omitempty"`
	BalanceRaw string `json:"balance_raw,omitempty"`
	Balance    string `json:"balance,omitempty"`
	Count      string `json:"count,omitempty"`
	// Decimals is a small bounded integer, so it is a JSON number. Pointer so
	// 0 decimals is preserved and it can be omitted for kinds that lack it.
	Decimals *int `json:"decimals,omitempty"`

	PreviousWei   string `json:"previous_wei,omitempty"`
	PreviousRaw   string `json:"previous_raw,omitempty"`
	PreviousCount string `json:"previous_count,omitempty"`
}

// OwnershipData is the payload for "ownership_sample" / "ownership_change"
// records: ERC-721 ownership of a specific token.
type OwnershipData struct {
	Kind          Kind   `json:"kind"`
	Token         string `json:"token"`
	TokenID       string `json:"token_id"`
	Owner         string `json:"owner"`
	PreviousOwner string `json:"previous_owner,omitempty"`
}

// ContractData is the payload for "contract_sample" / "contract_change"
// records. The populated subset depends on Field:
//
//   - native_balance:      BalanceWei, Balance.
//   - token_total_supply:  TotalSupplyRaw, TotalSupply, Decimals.
//   - transfer_count:      Count, WindowBlocks.
//
// Change records additionally set the matching Previous* field.
type ContractData struct {
	Address string `json:"address"`
	Field   Field  `json:"field"`

	BalanceWei     string `json:"balance_wei,omitempty"`
	Balance        string `json:"balance,omitempty"`
	TotalSupplyRaw string `json:"total_supply_raw,omitempty"`
	TotalSupply    string `json:"total_supply,omitempty"`
	Count          string `json:"count,omitempty"`
	// Decimals and WindowBlocks are small bounded integers (JSON numbers).
	Decimals     *int `json:"decimals,omitempty"`
	WindowBlocks *int `json:"window_blocks,omitempty"`

	PreviousWei         string `json:"previous_wei,omitempty"`
	PreviousRaw         string `json:"previous_raw,omitempty"`
	PreviousCount       string `json:"previous_count,omitempty"`
	PreviousTotalSupply string `json:"previous_total_supply,omitempty"`
}
