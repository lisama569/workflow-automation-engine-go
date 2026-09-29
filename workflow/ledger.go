package workflow

import (
	"context"
	"sync"
)

// Ledger simulates a side-effectful external dependency (payment, ticket
// creation, ...) that cannot be rolled back. Each business key can take
// effect exactly once: repeated calls with the same key return the stored
// result of the first successful call without performing the effect again.
type Ledger struct{}

// LedgerResult is the stored outcome of one idempotent key.
type LedgerResult struct {
	Output    map[string]any `json:"output"`
	UpdatedAt string         `json:"updated_at"`
}

// NewLedger opens (or creates) a ledger backed by dir.
// All processes sharing dir share the same ledger.
func NewLedger(dir string) (*Ledger, error) { return &Ledger{}, nil }

// Apply executes fn once per key.
//
//   - First call: fn runs; its success (and only its success) is recorded.
//   - Repeated call: fn is NOT executed; the stored result is returned.
//   - fn failure: nothing is recorded, so a later retry calls fn again.
func (l *Ledger) Apply(ctx context.Context, key string, fn func() (map[string]any, error)) (*LedgerResult, bool, error) {
	return nil, false, nil
}

// Entries returns a snapshot of all applied keys (for tests / inspection).
func (l *Ledger) Entries() map[string]LedgerResult { return nil }

// Count returns how many distinct keys have taken effect.
func (l *Ledger) Count() int { return 0 }

// InMemoryLedger is a non-persistent ledger for unit tests.
type InMemoryLedger struct {
	mu      sync.Mutex
	entries map[string]LedgerResult
}

// NewInMemoryLedger creates an empty in-memory ledger.
func NewInMemoryLedger() *InMemoryLedger { return &InMemoryLedger{} }

func (l *InMemoryLedger) Apply(ctx context.Context, key string, fn func() (map[string]any, error)) (*LedgerResult, bool, error) {
	return nil, false, nil
}

func (l *InMemoryLedger) Entries() map[string]LedgerResult { return nil }

func (l *InMemoryLedger) Count() int { return 0 }
