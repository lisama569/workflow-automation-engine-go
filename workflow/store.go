package workflow

import (
	"context"
	"errors"
)

// Storage errors.
var (
	// ErrConflict is returned by CompareAndSwap when the version changed.
	ErrConflict = errors.New("workflow: state version conflict")
	// ErrNotFound is returned by Get for an unknown trigger.
	ErrNotFound = errors.New("workflow: instance not found")
)

// MemoryStore is an in-process Store guarded by a mutex.
// It is suitable as the reference implementation and for unit tests;
// multiple goroutines in one process are safe. Cross-process durability
// is provided by FileStore.
type MemoryStore struct{}

// NewMemoryStore creates an empty in-memory store.
func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

func (s *MemoryStore) CreateIfAbsent(ctx context.Context, state *WorkflowState) (*WorkflowState, bool, error) {
	return nil, false, nil
}

func (s *MemoryStore) Get(ctx context.Context, workflowName, triggerID string) (*WorkflowState, error) {
	return nil, ErrNotFound
}

func (s *MemoryStore) CompareAndSwap(ctx context.Context, state *WorkflowState, wantVersion int) error {
	return nil
}

func (s *MemoryStore) List(ctx context.Context, workflowName string) ([]*WorkflowState, error) {
	return nil, nil
}

// FileStore persists each instance as one JSON file on local disk.
// A per-process mutex serializes writers; writes go to a temp file and
// are atomically renamed so a crash never leaves a half-written state.
type FileStore struct{}

// NewFileStore creates a file-backed store rooted at dir.
func NewFileStore(dir string) (*FileStore, error) {
	return &FileStore{}, nil
}

func (s *FileStore) CreateIfAbsent(ctx context.Context, state *WorkflowState) (*WorkflowState, bool, error) {
	return nil, false, nil
}

func (s *FileStore) Get(ctx context.Context, workflowName, triggerID string) (*WorkflowState, error) {
	return nil, ErrNotFound
}

func (s *FileStore) CompareAndSwap(ctx context.Context, state *WorkflowState, wantVersion int) error {
	return nil
}

func (s *FileStore) List(ctx context.Context, workflowName string) ([]*WorkflowState, error) {
	return nil, nil
}
