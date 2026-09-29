package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sync"

	"github.com/lisama569/workflow-automation-engine-go/pkg/workflow"
)

// FileStore 用本地 JSON 文件 + 进程内互斥模拟外部持久化依赖。
// 写入采用「临时文件 + rename」保证单文件原子性，进程崩溃不会留下半截 JSON。
type FileStore struct {
	dir string
	mu  sync.Mutex // 串行化同进程内的 CreateIfAbsent/Save，配合文件存在性做去重闸门
}

var safeName = regexp.MustCompile(`^[a-zA-Z0-9_.\-]+$`)

// NewFileStore 创建（必要时初始化目录）一个文件存储。
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create store dir: %w", err)
	}
	return &FileStore{dir: dir}, nil
}

func (s *FileStore) path(triggerID string) string {
	name := triggerID
	if !safeName.MatchString(name) {
		sum := sha256.Sum256([]byte(triggerID))
		name = "sha256-" + hex.EncodeToString(sum[:])
	}
	return filepath.Join(s.dir, name+".json")
}

// Load 读取一次触发对应的运行态；不存在返回 (nil, nil)。
func (s *FileStore) Load(ctx context.Context, triggerID string) (*workflow.RunState, error) {
	p := s.path(triggerID)
	b, err := os.ReadFile(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("load run state: %w", err)
	}
	var st workflow.RunState
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, fmt.Errorf("decode run state %s: %w", p, err)
	}
	return &st, nil
}

// CreateIfAbsent 仅当 triggerID 不存在时写入，返回当前已存在的状态（若有）。
// 用于并发/重复投递下的去重闸门：同一时刻只有一个调用能创建成功。
func (s *FileStore) CreateIfAbsent(ctx context.Context, state *workflow.RunState) (*workflow.RunState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	p := s.path(state.TriggerID)
	if _, err := os.Stat(p); err == nil {
		b, err := os.ReadFile(p)
		if err != nil {
			return nil, fmt.Errorf("read existing run state: %w", err)
		}
		var existing workflow.RunState
		if err := json.Unmarshal(b, &existing); err != nil {
			return nil, fmt.Errorf("decode existing run state: %w", err)
		}
		return &existing, nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("stat run state: %w", err)
	}
	if err := s.writeLocked(p, state); err != nil {
		return nil, err
	}
	return nil, nil
}

// Save 全量覆盖保存运行态。
func (s *FileStore) Save(ctx context.Context, state *workflow.RunState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeLocked(s.path(state.TriggerID), state)
}

func (s *FileStore) writeLocked(p string, state *workflow.RunState) error {
	b, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode run state: %w", err)
	}
	tmp := p + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create temp state file: %w", err)
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return fmt.Errorf("write temp state file: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("sync temp state file: %w", err)
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, p); err != nil {
		return fmt.Errorf("rename temp state file: %w", err)
	}
	return nil
}
