package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Ledger simulates a side-effectful external dependency (payment, ticket
// creation, ...) that cannot be rolled back. Each business key can take
// effect exactly once: repeated calls with the same key return the stored
// result of the first successful call without performing the effect again.
//
// It is the local stand-in for an "exactly-once effect" boundary and is the
// reason duplicate deliveries / retries never duplicate side effects:
// the effect function runs at most once per key, even across processes.
type Ledger struct {
	path string
	lock string
}

// LedgerResult is the stored outcome of one idempotent key.
type LedgerResult struct {
	Output    map[string]any `json:"output"`
	UpdatedAt string         `json:"updated_at"`
}

type ledgerFile struct {
	Entries map[string]LedgerResult `json:"entries"`
}

// NewLedger opens (or creates) a ledger backed by one JSON file in dir.
// All processes sharing dir share the same ledger (guarded by flock).
func NewLedger(dir string) (*Ledger, error) {
	if err := os.MkdirAll(filepath.Join(dir, "ledger"), 0o755); err != nil {
		return nil, fmt.Errorf("create ledger dir: %w", err)
	}
	l := &Ledger{
		path: filepath.Join(dir, "ledger", "effects.json"),
		lock: filepath.Join(dir, "ledger", "effects.lock"),
	}
	if _, err := os.Stat(l.path); errors.Is(err, os.ErrNotExist) {
		if err := atomicWriteJSON(l.path, ledgerFile{Entries: map[string]LedgerResult{}}); err != nil {
			return nil, err
		}
	}
	return l, nil
}

func (l *Ledger) loadLocked() (*ledgerFile, error) {
	data, err := os.ReadFile(l.path)
	if err != nil {
		return nil, err
	}
	f := &ledgerFile{}
	if err := json.Unmarshal(data, f); err != nil {
		return nil, fmt.Errorf("corrupt ledger: %w", err)
	}
	if f.Entries == nil {
		f.Entries = map[string]LedgerResult{}
	}
	return f, nil
}

// Apply executes fn once per key.
//
//   - First call: fn runs; only a success is recorded and persisted before
//     the method returns, so a later crash still sees the effect.
//   - Repeated call: fn is NOT executed; the stored result, reused=true is
//     returned. This is the duplicate-delivery / retry path.
//   - fn failure: nothing is recorded, so a later attempt calls fn again.
//
// The whole read-check-execute-write sequence holds an exclusive flock,
// making it safe across goroutines AND OS processes.
func (l *Ledger) Apply(ctx context.Context, key string, fn func() (map[string]any, error)) (result *LedgerResult, reused bool, err error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	lock, err := fileLock(l.lock)
	if err != nil {
		return nil, false, err
	}
	defer unlock(lock)

	f, err := l.loadLocked()
	if err != nil {
		return nil, false, err
	}
	if existing, ok := f.Entries[key]; ok {
		r := existing
		return &r, true, nil
	}
	output, err := fn()
	if err != nil {
		return nil, false, err
	}
	res := LedgerResult{Output: output, UpdatedAt: nowTS()}
	f.Entries[key] = res
	if err := atomicWriteJSON(l.path, f); err != nil {
		return nil, false, err
	}
	return &res, false, nil
}

// Entries returns a snapshot of all applied keys (for tests / inspection).
func (l *Ledger) Entries() (map[string]LedgerResult, error) {
	lock, err := fileLock(l.lock)
	if err != nil {
		return nil, err
	}
	defer unlock(lock)
	f, err := l.loadLocked()
	if err != nil {
		return nil, err
	}
	out := make(map[string]LedgerResult, len(f.Entries))
	for k, v := range f.Entries {
		out[k] = v
	}
	return out, nil
}

// Count returns how many distinct keys have taken effect.
func (l *Ledger) Count() (int, error) {
	entries, err := l.Entries()
	if err != nil {
		return 0, err
	}
	return len(entries), nil
}

// ---------------------------------------------------------------------------
// InMemoryLedger
// ---------------------------------------------------------------------------

// InMemoryLedger is a non-persistent ledger for unit tests.
type InMemoryLedger struct {
	mu      sync.Mutex
	entries map[string]LedgerResult
}

// NewInMemoryLedger creates an empty in-memory ledger.
func NewInMemoryLedger() *InMemoryLedger {
	return &InMemoryLedger{entries: map[string]LedgerResult{}}
}

func (l *InMemoryLedger) Apply(ctx context.Context, key string, fn func() (map[string]any, error)) (*LedgerResult, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if existing, ok := l.entries[key]; ok {
		r := existing
		return &r, true, nil
	}
	output, err := fn()
	if err != nil {
		return nil, false, err
	}
	res := LedgerResult{Output: output, UpdatedAt: nowTS()}
	l.entries[key] = res
	r := res
	return &r, false, nil
}

func (l *InMemoryLedger) Entries() map[string]LedgerResult {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[string]LedgerResult, len(l.entries))
	for k, v := range l.entries {
		out[k] = v
	}
	return out
}

func (l *InMemoryLedger) Count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

// EffectLedger is the abstraction activities depend on. Both Ledger and
// InMemoryLedger satisfy it.
type EffectLedger interface {
	Apply(ctx context.Context, key string, fn func() (map[string]any, error)) (*LedgerResult, bool, error)
}

// ---------------------------------------------------------------------------
// Persistent counter (used by deterministic demo activities)
// ---------------------------------------------------------------------------

type counterFile struct {
	Counters map[string]int `json:"counters"`
}

// CounterPath returns the path used by persisted counters.
func (l *Ledger) counterPath() string { return filepath.Join(filepath.Dir(l.path), "counters.json") }

// IncrementCounter atomically adds delta (typically 1) to a named counter
// and returns the new value. A missing counter is treated as zero.
func (l *Ledger) IncrementCounter(ctx context.Context, name string, delta int) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	lockPath := filepath.Join(filepath.Dir(l.path), "counters.lock")
	lock, err := fileLock(lockPath)
	if err != nil {
		return 0, err
	}
	defer unlock(lock)

	f := counterFile{Counters: map[string]int{}}
	if data, err := os.ReadFile(l.counterPath()); err == nil {
		if err := json.Unmarshal(data, &f); err != nil {
			return 0, fmt.Errorf("corrupt counters: %w", err)
		}
		if f.Counters == nil {
			f.Counters = map[string]int{}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return 0, err
	}
	f.Counters[name] += delta
	if err := atomicWriteJSON(l.counterPath(), f); err != nil {
		return 0, err
	}
	return f.Counters[name], nil
}

// Counter reads a named counter without changing it.
func (l *Ledger) Counter(ctx context.Context, name string) (int, error) {
	lockPath := filepath.Join(filepath.Dir(l.path), "counters.lock")
	lock, err := fileLock(lockPath)
	if err != nil {
		return 0, err
	}
	defer unlock(lock)
	f := counterFile{Counters: map[string]int{}}
	if data, err := os.ReadFile(l.counterPath()); err == nil {
		if err := json.Unmarshal(data, &f); err != nil {
			return 0, fmt.Errorf("corrupt counters: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return 0, err
	}
	return f.Counters[name], nil
}
