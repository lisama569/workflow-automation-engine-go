package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// Storage errors.
var (
	// ErrConflict is returned by CompareAndSwap when the version changed.
	ErrConflict = errors.New("workflow: state version conflict")
	// ErrNotFound is returned by Get for an unknown trigger.
	ErrNotFound = errors.New("workflow: instance not found")
)

// ---------------------------------------------------------------------------
// MemoryStore
// ---------------------------------------------------------------------------

// MemoryStore is an in-process Store guarded by a mutex.
// It is the reference implementation and is safe for concurrent goroutines.
// Cross-process durability is provided by FileStore.
type MemoryStore struct {
	mu       sync.Mutex
	data     map[string]*WorkflowState // key: workflowName/triggerID
	nextVer  map[string]int
}

func memKey(workflowName, triggerID string) string {
	return workflowName + "\x00" + triggerID
}

// NewMemoryStore creates an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		data:    map[string]*WorkflowState{},
		nextVer: map[string]int{},
	}
}

func (s *MemoryStore) CreateIfAbsent(ctx context.Context, state *WorkflowState) (*WorkflowState, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := memKey(state.WorkflowName, state.TriggerID)
	if existing, ok := s.data[key]; ok {
		return cloneState(existing), true, nil
	}
	s.data[key] = cloneState(state)
	return cloneState(state), false, nil
}

func (s *MemoryStore) Get(ctx context.Context, workflowName, triggerID string) (*WorkflowState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.data[memKey(workflowName, triggerID)]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneState(existing), nil
}

func (s *MemoryStore) CompareAndSwap(ctx context.Context, state *WorkflowState, wantVersion int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := memKey(state.WorkflowName, state.TriggerID)
	existing, ok := s.data[key]
	if !ok {
		return ErrNotFound
	}
	if existing.Version != wantVersion {
		return ErrConflict
	}
	next := cloneState(state)
	next.Version = wantVersion + 1
	s.data[key] = next
	return nil
}

func (s *MemoryStore) List(ctx context.Context, workflowName string) ([]*WorkflowState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*WorkflowState
	prefix := workflowName + "\x00"
	for k, v := range s.data {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			out = append(out, cloneState(v))
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// FileStore
// ---------------------------------------------------------------------------

// FileStore persists each instance as one JSON file on local disk.
//
// Writes go to a temp file and are atomically renamed. A per-instance
// advisory file lock (flock) serializes CreateIfAbsent/CompareAndSwap even
// across operating-system processes, so two processes restarting at the
// same time cannot both think they own the same trigger.
type FileStore struct {
	dir string
}

// NewFileStore creates a file-backed store rooted at dir.
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(filepath.Join(dir, "workflows"), 0o755); err != nil {
		return nil, fmt.Errorf("create store dir: %w", err)
	}
	return &FileStore{dir: dir}, nil
}

func (s *FileStore) dirFor(workflowName string) string {
	return filepath.Join(s.dir, "workflows", safeName(workflowName))
}

func (s *FileStore) statePath(workflowName, triggerID string) string {
	return filepath.Join(s.dirFor(workflowName), safeName(triggerID)+".json")
}

func (s *FileStore) lockPath(workflowName, triggerID string) string {
	return filepath.Join(s.dirFor(workflowName), safeName(triggerID)+".lock")
}

// fileLock acquires an exclusive flock and returns the file to close later.
func fileLock(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func unlock(f *os.File) {
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	_ = f.Close()
}

func (s *FileStore) CreateIfAbsent(ctx context.Context, state *WorkflowState) (*WorkflowState, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	lock, err := fileLock(s.lockPath(state.WorkflowName, state.TriggerID))
	if err != nil {
		return nil, false, err
	}
	defer unlock(lock)

	path := s.statePath(state.WorkflowName, state.TriggerID)
	if data, err := os.ReadFile(path); err == nil {
		existing := &WorkflowState{}
		if err := json.Unmarshal(data, existing); err != nil {
			return nil, false, fmt.Errorf("corrupt state file %s: %w", path, err)
		}
		return existing, true, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, false, err
	}
	if err := atomicWriteJSON(path, state); err != nil {
		return nil, false, err
	}
	return cloneState(state), false, nil
}

func (s *FileStore) Get(ctx context.Context, workflowName, triggerID string) (*WorkflowState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(s.statePath(workflowName, triggerID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	state := &WorkflowState{}
	if err := json.Unmarshal(data, state); err != nil {
		return nil, fmt.Errorf("corrupt state: %w", err)
	}
	return state, nil
}

func (s *FileStore) CompareAndSwap(ctx context.Context, state *WorkflowState, wantVersion int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	lock, err := fileLock(s.lockPath(state.WorkflowName, state.TriggerID))
	if err != nil {
		return err
	}
	defer unlock(lock)

	path := s.statePath(state.WorkflowName, state.TriggerID)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrNotFound
		}
		return err
	}
	current := &WorkflowState{}
	if err := json.Unmarshal(data, current); err != nil {
		return fmt.Errorf("corrupt state: %w", err)
	}
	if current.Version != wantVersion {
		return ErrConflict
	}
	next := cloneState(state)
	next.Version = wantVersion + 1
	return atomicWriteJSON(path, next)
}

func (s *FileStore) List(ctx context.Context, workflowName string) ([]*WorkflowState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(s.dirFor(workflowName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var out []*WorkflowState
	for _, en := range entries {
		if en.IsDir() || filepath.Ext(en.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.dirFor(workflowName), en.Name()))
		if err != nil {
			return nil, err
		}
		st := &WorkflowState{}
		if err := json.Unmarshal(data, st); err != nil {
			return nil, fmt.Errorf("corrupt state file %s: %w", en.Name(), err)
		}
		out = append(out, st)
	}
	return out, nil
}

// atomicWriteJSON writes v to path via a temp file + rename.
func atomicWriteJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}
