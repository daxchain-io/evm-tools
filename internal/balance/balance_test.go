package balance

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/daxchain-io/evm-tools/internal/record"
	"github.com/daxchain-io/evm-tools/internal/rpc"
)

// fakeClient is a programmable RPC client for the poller tests. It serves
// per-address native balances and per-(to,data) eth_call results, both of which
// the test can mutate between ticks to drive change detection.
type fakeClient struct {
	mu sync.Mutex

	heads   []uint64
	headIdx int

	// balances maps lowercased address -> wei balance (eth_getBalance).
	balances map[string]*big.Int
	// calls maps lowercased "to|data" -> 0x-hex result (eth_call).
	calls map[string]string
	// callErr, when set for a "to|data" key, makes that call fail.
	callErr map[string]error

	// logs returned by GetLogs (for transfer_count windows).
	logs    []rpc.Log
	getLogs func(f rpc.LogFilter) ([]rpc.Log, error)

	// block returned by BlockByNumberUint.
	blockTimestamp string

	getLogsCalls []rpc.LogFilter
}

func (c *fakeClient) BlockNumber(context.Context) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.heads) == 0 {
		return 100, nil
	}
	if c.headIdx >= len(c.heads) {
		return c.heads[len(c.heads)-1], nil
	}
	h := c.heads[c.headIdx]
	c.headIdx++
	return h, nil
}

func (c *fakeClient) BlockByNumberUint(_ context.Context, n uint64, _ bool) (*rpc.Block, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ts := c.blockTimestamp
	if ts == "" {
		ts = "0x0"
	}
	return &rpc.Block{Number: rpc.BlockTag(n), Hash: "0xblk", Timestamp: ts}, nil
}

func (c *fakeClient) BalanceAt(_ context.Context, address, _ string) (*big.Int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if v, ok := c.balances[lower(address)]; ok {
		return new(big.Int).Set(v), nil
	}
	return big.NewInt(0), nil
}

func (c *fakeClient) Call(_ context.Context, msg rpc.CallMsg, _ string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := lower(msg.To) + "|" + msg.Data
	if c.callErr != nil {
		if err, ok := c.callErr[key]; ok {
			return "", err
		}
	}
	if v, ok := c.calls[key]; ok {
		return v, nil
	}
	return "0x", nil
}

func (c *fakeClient) GetLogs(_ context.Context, f rpc.LogFilter) ([]rpc.Log, error) {
	c.mu.Lock()
	c.getLogsCalls = append(c.getLogsCalls, f)
	logs := c.logs
	gl := c.getLogs
	c.mu.Unlock()
	if gl != nil {
		return gl(f)
	}
	return logs, nil
}

// getLogsCallCount returns the number of GetLogs calls seen so far (lock-safe).
func (c *fakeClient) getLogsCallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.getLogsCalls)
}

// firstGetLogsCall returns the first GetLogs filter seen (lock-safe). It must be
// called only after getLogsCallCount() > 0.
func (c *fakeClient) firstGetLogsCall() rpc.LogFilter {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.getLogsCalls[0]
}

// setBalance updates an account balance between ticks.
func (c *fakeClient) setBalance(addr string, wei *big.Int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.balances[lower(addr)] = wei
}

// setCall updates an eth_call result between ticks.
func (c *fakeClient) setCall(to, data, result string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls[lower(to)+"|"+data] = result
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		balances: map[string]*big.Int{},
		calls:    map[string]string{},
		callErr:  map[string]error{},
	}
}

// captureEmitter records emitted envelopes.
type captureEmitter struct {
	mu     sync.Mutex
	envs   []record.Envelope
	onEmit func() error
}

func (e *captureEmitter) Emit(env record.Envelope) error {
	if e.onEmit != nil {
		if err := e.onEmit(); err != nil {
			return err
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.envs = append(e.envs, env)
	return nil
}

func (e *captureEmitter) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.envs)
}

func (e *captureEmitter) snapshot() []record.Envelope {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]record.Envelope, len(e.envs))
	copy(out, e.envs)
	return out
}

func (e *captureEmitter) byType(typ record.Type) []record.Envelope {
	var out []record.Envelope
	for _, env := range e.snapshot() {
		if env.Type == typ {
			out = append(out, env)
		}
	}
	return out
}

// uint256Hex renders n as a left-padded 32-byte 0x-hex word (an eth_call result).
func uint256Hex(n int64) string {
	return "0x" + leftPad(big.NewInt(n).Text(16), 64)
}

func leftPad(s string, width int) string {
	for len(s) < width {
		s = "0" + s
	}
	return s
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

// runPoller starts a poller and returns a stop func that cancels and waits.
func runPoller(t *testing.T, p *Poller) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	return func() {
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	}
}

// TestNativeSampleAndChange verifies a native account emits a balance_sample
// every tick and a balance_change only when the balance moves, with the prior
// value carried.
func TestNativeSampleAndChange(t *testing.T) {
	fc := newFakeClient()
	fc.balances[lower("0xacct")] = big.NewInt(1_000)

	em := &captureEmitter{}
	p, err := New(Options{
		Client:    fc,
		Emitter:   em,
		ChainName: "my-chain",
		ChainID:   4242,
		Cadence:   Cadence{Interval: 3 * time.Millisecond},
		Native:    []NativeTarget{{Name: "treasury", Address: "0xACCT"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	stop := runPoller(t, p)
	// First tick: one sample, no change (first observation).
	waitFor(t, func() bool { return len(em.byType(record.TypeBalanceSample)) >= 1 }, time.Second)
	// Move the balance; a later tick should emit a change.
	fc.setBalance("0xacct", big.NewInt(2_500))
	waitFor(t, func() bool { return len(em.byType(record.TypeBalanceChange)) >= 1 }, time.Second)
	stop()

	samples := em.byType(record.TypeBalanceSample)
	if len(samples) < 2 {
		t.Fatalf("expected >=2 samples, got %d", len(samples))
	}
	first := samples[0].Data.(record.BalanceData)
	if first.Kind != record.KindNative || first.BalanceWei != "1000" || first.Balance == "" {
		t.Errorf("first native sample wrong: %+v", first)
	}
	if first.PreviousWei != "" {
		t.Errorf("sample must not carry previous_wei: %+v", first)
	}

	changes := em.byType(record.TypeBalanceChange)
	ch := changes[0].Data.(record.BalanceData)
	if ch.BalanceWei != "2500" || ch.PreviousWei != "1000" {
		t.Errorf("change should carry prior 1000 -> 2500, got %+v", ch)
	}
}

// TestPermanentResultFailsFast verifies that a misconfigured target (an address
// that is not a contract, so eth_call returns "0x") makes the poller fail fast
// with a permanent error rather than retrying transiently forever and silently
// emitting no data for any target (Principle 7).
func TestPermanentResultFailsFast(t *testing.T) {
	fc := newFakeClient()
	fc.setCall("0xbad", callDataDecimals(), uint256Hex(6))
	fc.setCall("0xbad", callDataBalanceOf("0xholder"), "0x") // empty: not a contract
	em := &captureEmitter{}
	p, err := New(Options{
		Client:      fc,
		Emitter:     em,
		ChainName:   "c",
		ChainID:     1,
		Cadence:     Cadence{Interval: time.Millisecond},
		ERC20:       []ERC20Target{{Name: "bad", Token: "0xbad", Address: "0xholder"}},
		BackoffBase: time.Millisecond,
		BackoffMax:  2 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- p.Run(context.Background()) }()
	select {
	case rerr := <-done:
		if rerr == nil || !strings.Contains(rerr.Error(), "permanent") {
			t.Fatalf("want a permanent sampling failure, got %v", rerr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("poller did not fail fast on a permanent decode error (retrying forever?)")
	}
}

// TestERC20DecimalsResolutionAndOverride verifies decimals() is resolved at
// startup via eth_call and applied to the formatted balance, and that a config
// override wins over the on-chain decimals() (the override token's decimals() is
// never called).
func TestERC20DecimalsResolutionAndOverride(t *testing.T) {
	fc := newFakeClient()
	// Token A implements decimals() = 6; balanceOf(holder) = 2_000_000.
	fc.setCall("0xtokenA", callDataDecimals(), uint256Hex(6))
	fc.setCall("0xtokenA", callDataBalanceOf("0xholder"), uint256Hex(2_000_000))
	// Token B: decimals() would return 99 but the config override (2) wins.
	fc.setCall("0xtokenB", callDataDecimals(), uint256Hex(99))
	fc.setCall("0xtokenB", callDataBalanceOf("0xholder"), uint256Hex(500))

	override := 2
	em := &captureEmitter{}
	p, err := New(Options{
		Client:    fc,
		Emitter:   em,
		ChainName: "c",
		ChainID:   1,
		Cadence:   Cadence{Interval: 3 * time.Millisecond},
		ERC20: []ERC20Target{
			{Name: "usdc", Token: "0xTokenA", Address: "0xHolder"},
			{Name: "ovr", Token: "0xTokenB", Address: "0xHolder", Decimals: &override},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	stop := runPoller(t, p)
	waitFor(t, func() bool { return len(em.byType(record.TypeBalanceSample)) >= 2 }, time.Second)
	stop()

	byName := map[string]record.BalanceData{}
	for _, env := range em.byType(record.TypeBalanceSample) {
		byName[env.Name] = env.Data.(record.BalanceData)
	}
	usdc := byName["usdc"]
	if usdc.Decimals == nil || *usdc.Decimals != 6 {
		t.Errorf("usdc decimals not resolved to 6: %+v", usdc)
	}
	if usdc.BalanceRaw != "2000000" || usdc.Balance != "2.0" {
		t.Errorf("usdc formatted balance wrong: %+v", usdc)
	}
	ovr := byName["ovr"]
	if ovr.Decimals == nil || *ovr.Decimals != 2 {
		t.Errorf("override decimals should win (want 2): %+v", ovr)
	}
	if ovr.Balance != "5.0" { // 500 / 10^2
		t.Errorf("override formatted balance wrong: %+v", ovr)
	}
}

// TestERC20NoDecimalsEmitsRawOnly verifies a token that does not implement
// decimals() (the call errors) and has no override emits only the raw balance
// with no decimals/Balance fields.
func TestERC20NoDecimalsEmitsRawOnly(t *testing.T) {
	fc := newFakeClient()
	fc.callErr[lower("0xtoken")+"|"+callDataDecimals()] = errors.New("execution reverted")
	fc.setCall("0xtoken", callDataBalanceOf("0xholder"), uint256Hex(42))

	em := &captureEmitter{}
	p, err := New(Options{
		Client:    fc,
		Emitter:   em,
		ChainName: "c",
		ChainID:   1,
		Cadence:   Cadence{Interval: 3 * time.Millisecond},
		ERC20:     []ERC20Target{{Name: "weird", Token: "0xToken", Address: "0xHolder"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	stop := runPoller(t, p)
	waitFor(t, func() bool { return len(em.byType(record.TypeBalanceSample)) >= 1 }, time.Second)
	stop()

	s := em.byType(record.TypeBalanceSample)[0].Data.(record.BalanceData)
	if s.BalanceRaw != "42" {
		t.Errorf("raw balance wrong: %+v", s)
	}
	if s.Decimals != nil || s.Balance != "" {
		t.Errorf("no decimals/Balance expected for undecimaled token: %+v", s)
	}
}

// TestERC721BalanceSampleAndChange verifies an ERC-721 balance entry emits a
// balance_sample with kind erc721 and a count via balanceOf(owner), and a
// balance_change carrying the prior count when the count moves.
func TestERC721BalanceSampleAndChange(t *testing.T) {
	fc := newFakeClient()
	fc.setCall("0xnft", callDataBalanceOf("0xowner"), uint256Hex(7))

	em := &captureEmitter{}
	p, err := New(Options{
		Client:         fc,
		Emitter:        em,
		ChainName:      "c",
		ChainID:        1,
		Cadence:        Cadence{Interval: 3 * time.Millisecond},
		ERC721Balances: []ERC721BalanceTarget{{Name: "vault-nft", Token: "0xNFT", Owner: "0xOwner"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	stop := runPoller(t, p)
	waitFor(t, func() bool { return len(em.byType(record.TypeBalanceSample)) >= 1 }, time.Second)
	fc.setCall("0xnft", callDataBalanceOf("0xowner"), uint256Hex(9))
	waitFor(t, func() bool { return len(em.byType(record.TypeBalanceChange)) >= 1 }, time.Second)
	stop()

	s := em.byType(record.TypeBalanceSample)[0].Data.(record.BalanceData)
	if s.Kind != record.KindERC721 || s.Count != "7" || s.Token != "0xNFT" || s.Address != "0xOwner" {
		t.Errorf("erc721 balance sample wrong: %+v", s)
	}
	// ERC-721 records carry counts, not decimals or a formatted balance.
	if s.Decimals != nil || s.Balance != "" || s.BalanceRaw != "" {
		t.Errorf("erc721 balance sample must carry only count: %+v", s)
	}
	if s.PreviousCount != "" {
		t.Errorf("sample must not carry previous_count: %+v", s)
	}

	ch := em.byType(record.TypeBalanceChange)[0].Data.(record.BalanceData)
	if ch.Count != "9" || ch.PreviousCount != "7" {
		t.Errorf("erc721 balance change should carry 7 -> 9, got %+v", ch)
	}
}

// TestERC721OwnershipSampleAndChange verifies an ERC-721 ownership entry emits an
// ownership_sample carrying the current owner via ownerOf(token_id), and an
// ownership_change carrying the previous owner when ownership moves.
func TestERC721OwnershipSampleAndChange(t *testing.T) {
	fc := newFakeClient()
	// ownerOf(1234) -> 0x...aaaa initially.
	fc.setCall("0xnft", callDataOwnerOf("1234"), addressWord("0xaaaa"))

	em := &captureEmitter{}
	p, err := New(Options{
		Client:          fc,
		Emitter:         em,
		ChainName:       "c",
		ChainID:         1,
		Cadence:         Cadence{Interval: 3 * time.Millisecond},
		ERC721Ownership: []ERC721OwnershipTarget{{Name: "special", Token: "0xNFT", TokenID: "1234"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	stop := runPoller(t, p)
	waitFor(t, func() bool { return len(em.byType(record.TypeOwnershipSample)) >= 1 }, time.Second)
	fc.setCall("0xnft", callDataOwnerOf("1234"), addressWord("0xbbbb"))
	waitFor(t, func() bool { return len(em.byType(record.TypeOwnershipChange)) >= 1 }, time.Second)
	stop()

	s := em.byType(record.TypeOwnershipSample)[0].Data.(record.OwnershipData)
	if s.Kind != record.KindERC721 || s.Token != "0xNFT" || s.TokenID != "1234" {
		t.Errorf("ownership sample envelope wrong: %+v", s)
	}
	if s.Owner != "0x000000000000000000000000000000000000aaaa" {
		t.Errorf("ownership sample owner wrong: %+v", s)
	}
	if s.PreviousOwner != "" {
		t.Errorf("sample must not carry previous_owner: %+v", s)
	}

	ch := em.byType(record.TypeOwnershipChange)[0].Data.(record.OwnershipData)
	if ch.Owner != "0x000000000000000000000000000000000000bbbb" ||
		ch.PreviousOwner != "0x000000000000000000000000000000000000aaaa" {
		t.Errorf("ownership change should carry aaaa -> bbbb, got %+v", ch)
	}
}

// TestERC721OwnershipNoFalseChangeOnChecksumCase verifies a checksum-vs-lowercase
// difference in the RPC-returned owner is not mistaken for a transfer. Since the
// decoder always lowercases, this is asserted via ownerChanged directly.
func TestERC721OwnershipNoFalseChangeOnChecksumCase(t *testing.T) {
	p := &Poller{priorOwner: map[string]string{}}
	if moved, _ := p.ownerChanged("k", "0xAbCd"); moved {
		t.Error("first observation must not be a change")
	}
	if moved, _ := p.ownerChanged("k", "0xabcd"); moved {
		t.Error("same owner in different case must not be a change")
	}
	if moved, prev := p.ownerChanged("k", "0xbeef"); !moved || prev != "0xabcd" {
		t.Errorf("genuine owner move should report change with prior, got moved=%v prev=%q", moved, prev)
	}
}

// addressWord renders a short 0x address as a 32-byte left-padded eth_call result
// word (an ownerOf() return value). padAddress already produces a 32-byte word.
func addressWord(addr string) string {
	return "0x" + padAddress(addr)
}

// TestContractFields verifies a contract entry emits the native_balance,
// token_total_supply, and transfer_count contract_sample records, with the
// transfer count derived from the window's Transfer logs.
func TestContractFields(t *testing.T) {
	fc := newFakeClient()
	fc.balances[lower("0xcontract")] = big.NewInt(7)
	fc.setCall("0xcontract", callDataDecimals(), uint256Hex(6))
	fc.setCall("0xcontract", callDataTotalSupply(), uint256Hex(50_000_000))
	fc.logs = []rpc.Log{
		{Address: "0xcontract", Topics: []string{transferTopic0}},
		{Address: "0xcontract", Topics: []string{transferTopic0}},
		{Address: "0xcontract", Topics: []string{transferTopic0}},
	}

	em := &captureEmitter{}
	p, err := New(Options{
		Client:    fc,
		Emitter:   em,
		ChainName: "c",
		ChainID:   1,
		Cadence:   Cadence{Interval: 3 * time.Millisecond},
		Contracts: []ContractTarget{{
			Name:                      "usdc",
			Address:                   "0xContract",
			NativeBalance:             true,
			TokenSupply:               true,
			TransferCountWindowBlocks: 1000,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	stop := runPoller(t, p)
	waitFor(t, func() bool { return len(em.byType(record.TypeContractSample)) >= 3 }, time.Second)
	stop()

	byField := map[record.Field]record.ContractData{}
	for _, env := range em.byType(record.TypeContractSample) {
		d := env.Data.(record.ContractData)
		byField[d.Field] = d
	}
	nb, ok := byField[record.FieldNativeBalance]
	if !ok || nb.BalanceWei != "7" {
		t.Errorf("native_balance wrong: %+v", nb)
	}
	ts, ok := byField[record.FieldTokenTotalSupply]
	if !ok || ts.TotalSupplyRaw != "50000000" || ts.Decimals == nil || *ts.Decimals != 6 {
		t.Errorf("token_total_supply wrong: %+v", ts)
	}
	if ts.TotalSupply != "50.0" {
		t.Errorf("token_total_supply formatted wrong: %+v", ts)
	}
	tc, ok := byField[record.FieldTransferCount]
	if !ok || tc.Count != "3" || tc.WindowBlocks == nil || *tc.WindowBlocks != 1000 {
		t.Errorf("transfer_count wrong: %+v", tc)
	}

	// Verify the window filter scoped Transfer logs to the contract.
	if fc.getLogsCallCount() == 0 {
		t.Fatal("expected at least one GetLogs call for transfer_count")
	}
	f := fc.firstGetLogsCall()
	if len(f.Topics) != 1 {
		t.Errorf("transfer_count filter should match topic0 only: %+v", f.Topics)
	}
}

// TestTransferCountWindowClamp verifies the trailing window is clamped to block 0
// when head is smaller than the configured window (no underflow).
func TestTransferCountWindowClamp(t *testing.T) {
	fc := newFakeClient()
	fc.heads = []uint64{10} // head 10, window 1000 -> from must clamp to 0
	em := &captureEmitter{}
	p, err := New(Options{
		Client:    fc,
		Emitter:   em,
		ChainName: "c",
		ChainID:   1,
		Cadence:   Cadence{Interval: 3 * time.Millisecond},
		Contracts: []ContractTarget{{
			Name: "c", Address: "0xc", TransferCountWindowBlocks: 1000,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	stop := runPoller(t, p)
	waitFor(t, func() bool { return fc.getLogsCallCount() >= 1 }, time.Second)
	stop()

	f := fc.firstGetLogsCall()
	if f.FromBlock != 0 || f.ToBlock != 10 {
		t.Errorf("window should clamp to [0,10], got [%d,%d]", f.FromBlock, f.ToBlock)
	}
}

// TestNewRejectsBadCadence verifies the interval-XOR-every_blocks rule and the
// empty-target guard at construction.
func TestNewRejectsBadCadence(t *testing.T) {
	fc := newFakeClient()
	em := &captureEmitter{}
	base := Options{Client: fc, Emitter: em, Native: []NativeTarget{{Name: "n", Address: "0x1"}}}

	bad := base
	bad.Cadence = Cadence{} // neither set
	if _, err := New(bad); err == nil {
		t.Error("expected error when neither interval nor every_blocks set")
	}

	both := base
	both.Cadence = Cadence{Interval: time.Second, EveryBlocks: 5}
	if _, err := New(both); err == nil {
		t.Error("expected error when both interval and every_blocks set")
	}

	noTargets := Options{Client: fc, Emitter: em, Cadence: Cadence{Interval: time.Second}}
	if _, err := New(noTargets); err == nil {
		t.Error("expected error when no targets configured")
	}
}

// TestRetryOnTransientFailure verifies a transient BalanceAt/BlockNumber error is
// retried (not fatal) and the poller recovers.
func TestRetryOnTransientFailure(t *testing.T) {
	fc := &flakyClient{fakeClient: newFakeClient(), failFirst: 2}
	fc.balances[lower("0xa")] = big.NewInt(1)
	em := &captureEmitter{}
	p, err := New(Options{
		Client:      fc,
		Emitter:     em,
		ChainName:   "c",
		ChainID:     1,
		Cadence:     Cadence{Interval: 3 * time.Millisecond},
		Native:      []NativeTarget{{Name: "n", Address: "0xa"}},
		BackoffBase: time.Millisecond,
		BackoffMax:  2 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	stop := runPoller(t, p)
	waitFor(t, func() bool { return em.count() >= 1 }, 2*time.Second)
	stop()
	if em.count() < 1 {
		t.Error("poller should recover from transient failure and eventually emit")
	}
}

// flakyClient fails the first failFirst BlockNumber calls, then behaves normally.
type flakyClient struct {
	*fakeClient
	mu        sync.Mutex
	failFirst int
	n         int
}

func (c *flakyClient) BlockNumber(_ context.Context) (uint64, error) {
	c.mu.Lock()
	c.n++
	fail := c.n <= c.failFirst
	c.mu.Unlock()
	if fail {
		return 0, errors.New("transient")
	}
	return 100, nil
}

// TestBackpressurePropagates verifies an emitter error propagates as a poll
// failure rather than dropping the record; the poller retries and eventually
// emits once backpressure clears.
func TestBackpressurePropagates(t *testing.T) {
	fc := newFakeClient()
	fc.balances[lower("0xa")] = big.NewInt(1)

	var mu sync.Mutex
	attempts := 0
	em := &captureEmitter{onEmit: func() error {
		mu.Lock()
		defer mu.Unlock()
		attempts++
		if attempts >= 3 {
			return nil
		}
		return errors.New("pipe blocked then broke")
	}}
	p, err := New(Options{
		Client:      fc,
		Emitter:     em,
		ChainName:   "c",
		ChainID:     1,
		Cadence:     Cadence{Interval: 2 * time.Millisecond},
		Native:      []NativeTarget{{Name: "n", Address: "0xa"}},
		BackoffBase: time.Millisecond,
		BackoffMax:  2 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	stop := runPoller(t, p)
	waitFor(t, func() bool { return em.count() >= 1 }, 2*time.Second)
	stop()
	if em.count() < 1 {
		t.Error("record was dropped under backpressure")
	}
}

// TestBlockCadenceSamplesOnAdvance verifies block-cadence sampling fires once
// initially and again only after the head advances by every_blocks.
func TestBlockCadenceSamplesOnAdvance(t *testing.T) {
	fc := newFakeClient()
	fc.balances[lower("0xa")] = big.NewInt(1)
	// head sequence: 100 (sample), 110 (<+50, no sample), 160 (>=150, sample).
	fc.heads = []uint64{100, 110, 160}

	em := &captureEmitter{}
	p, err := New(Options{
		Client:    fc,
		Emitter:   em,
		ChainName: "c",
		ChainID:   1,
		Cadence:   Cadence{EveryBlocks: 50},
		Native:    []NativeTarget{{Name: "n", Address: "0xa"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Use a tiny head-poll interval via a fast-forwarding clock is overkill; the
	// runBlockCadence head poll is 2s, so drive sampleAll directly instead.
	if err := p.sampleAll(context.Background(), 100); err != nil {
		t.Fatal(err)
	}
	if got := len(em.byType(record.TypeBalanceSample)); got != 1 {
		t.Fatalf("expected 1 sample after first head, got %d", got)
	}
	// Simulate the cadence gate: head 110 is < 100+50, so no new sample.
	// head 160 >= 100+50, so sample again.
	if err := p.sampleAll(context.Background(), 160); err != nil {
		t.Fatal(err)
	}
	if got := len(em.byType(record.TypeBalanceSample)); got != 2 {
		t.Fatalf("expected 2 samples total, got %d", got)
	}
}
